package org.cloudname.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.cloudname.CloudnameException;
import org.cloudname.Endpoint;
import org.cloudname.Resolver;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class that is capable of tracking an expression. An expression can include many nodes.
 * The number of nodes is dynamic and can change over time.
 * For now, the implementation is rather simple. For single endpoints it does use feedback from
 * ZooKeeper watcher events. For keeping track of new nodes, it does a scan on regular intervals.
 * @author dybdahl
 */
class DynamicExpression implements Watcher, TrackedCoordinate.ExpressionResolverNotify {

    /**
     * Keeps track of what picture (what an expression has resolved to) is sent to the user so that
     * we know when to send new events.
     */
    private final Map<String, Endpoint> clientPicture = new HashMap<String, Endpoint>();

    /**
     * Where to notify changes.
     */
    private final Resolver.ResolverListener clientCallback;

    /**
     * A path can be scheduled to be read later, i.e. if we think there are changes, we got error
     * etc.
     */
    private final Map<String, Long> scheduledRefreshMsByPath = new HashMap<String, Long>();

    /**
     * This is the expression to dynamically resolved represented as ZkResolver.Parameters.
     */
    private final ZkResolver.Parameters parameters;

    /**
     * When ZooKeeper reports an error about an path, when to try to read it again.
     */
    private final long RETRY_INTERVAL_ZOOKEEPER_ERROR_MS = 30000;      // 30 seconds

    /**
     * We wait a bit after a node has changed because in many cases there might be several updates,
     * e.g. an application registers several endpoints, each causing an update.
     */
    private final long REFRESH_NODE_AFTER_CHANGE_MS = 2000;            // two seconds

    /**
     * Does a full scan with this interval. Non-final so unit test can run faster.
     */
    protected static long TIME_BETWEEN_NODE_SCANNING_MS = 1 * 60 * 1000;  // one minute

    /**
     * For timing next node scan.
     */
    private long lastNodeScanMs = 0;

    /**
     * A Map with all the coordinate we care about for now.
     */
    final private Map<String, TrackedCoordinate> coordinateByPath =
            new HashMap<String, TrackedCoordinate>();

    /**
     * We always add some random noise to when to do things so not all servers fire at the same time
     * against
     * ZooKeeper.
     */
    private final Random random = new Random();
    
    private static final Logger log = Logger.getLogger(DynamicExpression.class.getName());

    private boolean stopped = false;

    private final ZkResolver zkResolver;

    private final ZkObjectHandler.Client zkClient;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    private final Object instanceLock = new Object();

    /**
     * Start getting notified about changes to expression.
     * @param expression Coordinate expression.
     * @param clientCallback called on changes and initially.
     */
    public DynamicExpression(
            final String expression,
            final Resolver.ResolverListener clientCallback,
            final ZkResolver zkResolver,
            final ZkObjectHandler.Client zkClient) {
        this.clientCallback = clientCallback;
        this.parameters = new ZkResolver.Parameters(expression);
        this.zkResolver = zkResolver;
        this.zkClient = zkClient;
    }

    /**
     * Stop receiving callbacks about coordinate.
     */
    public void stop() {
        scheduler.shutdown();

        synchronized (instanceLock) {
            stopped = true;
            for (TrackedCoordinate trackedCoordinate : coordinateByPath.values()) {
                trackedCoordinate.stop();
            }
            coordinateByPath.clear();
        }
    }

    private List<String> pullPathsToBeRefreshed() {
        final List<String> pathsToRefresh = new ArrayList<String>();
        synchronized (instanceLock) {
            long nowMillis = System.currentTimeMillis();
            for (Map.Entry<String, Long> entry : scheduledRefreshMsByPath.entrySet()) {
                if (nowMillis - entry.getValue()  >  0) {
                    pathsToRefresh.add(entry.getKey());
                }
            }
            for (String path : pathsToRefresh) {
                scheduledRefreshMsByPath.remove(path);
            }
        }
        return pathsToRefresh;
    }

    public void start() {
        final long periodicDelayMs = 500;
        scheduler.scheduleWithFixedDelay(new ResolveProblems(), 1 /* initial delay ms */,
                periodicDelayMs, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(new NodeScanner(), 1 /* initial delay ms */,
                TIME_BETWEEN_NODE_SCANNING_MS, TimeUnit.MILLISECONDS);
    }


    /**
     * The method will try to resolve the expression and find new nodes.
     */
    private class NodeScanner implements Runnable {
        @Override
        public void run() {
            resolve();
            notifyClient();

        }
    }

    /**
     * Check if there are paths that are scheduled for refresh.
     */
    private class ResolveProblems implements Runnable {
        @Override
        public void run() {
            // Periodically check every node for changes.
            List<String>  pathsToRefresh = pullPathsToBeRefreshed();
            for (String path : pathsToRefresh) {
                refreshPathWithWatcher(path);
            }
        }
    }

    /**
     * Callback from zookeeper watcher.
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        synchronized (instanceLock) {
            if (stopped) {
                return;
            }
        }
        String path = watchedEvent.getPath();
        Event.KeeperState state = watchedEvent.getState();
        Event.EventType type = watchedEvent.getType();

        switch (state) {
            case Expired:
            case AuthFailed:
            case Disconnected:
                // Something bad happened to the path, try again later.
                scheduleRefresh(path, RETRY_INTERVAL_ZOOKEEPER_ERROR_MS);
                break;
        }
        switch (type) {
            case NodeChildrenChanged:
            case None:
            case NodeCreated:
                scheduleRefresh(path, REFRESH_NODE_AFTER_CHANGE_MS);
                break;
            case NodeDeleted:
                synchronized (instanceLock) {
                    coordinateByPath.remove(path);
                    notifyClient();
                    scheduledRefreshMsByPath.remove(path);
                    return;
                }
            case NodeDataChanged:
                refreshPathWithWatcher(path);
                break;
        }

    }

    /**
     * Implements interface TrackedCoordinate.ExpressionResolverNotify
     */
    @Override
    public void stateChanged() {
        notifyClient();
    }

    private void resolve() {
        List<Endpoint> endpoints;
        synchronized (instanceLock) {
            try {
              endpoints = zkResolver.resolve(parameters.getExpression());
            } catch (CloudnameException e) {
                log.warning("Exception from cloudname: " + e.toString());
                return;
            }
        }

        Map<String, TrackedCoordinate> tempStatusAndEndpointsMap =
                new HashMap<String, TrackedCoordinate>();

        for (Endpoint endpoint : endpoints) {
            String statusPath = ZkCoordinatePath.getStatusPath(endpoint.getCoordinate());

            TrackedCoordinate trackedCoordinate = new TrackedCoordinate(this, statusPath, zkClient);
            trackedCoordinate.start();
            try {
                trackedCoordinate.waitForFirstData();
            } catch (InterruptedException e) {
                log.log(Level.SEVERE, "Got interrupt while waiting for data.", e);
                return;
            }

            tempStatusAndEndpointsMap.put(statusPath, trackedCoordinate);
        }

        synchronized (instanceLock) {
            for (TrackedCoordinate trackedCoordinate : coordinateByPath.values()) {
                trackedCoordinate.stop();
            }
            coordinateByPath.clear();
            coordinateByPath.putAll(tempStatusAndEndpointsMap);
        }
    }

    private String getEndpointKey(final Endpoint endpoint) {
        return endpoint.getCoordinate().asString() + "@" + endpoint.getName();
    }


    private List<Endpoint> getNewEndpoints() {
        List<Endpoint> newEndpoints = new ArrayList<Endpoint>();
        for (TrackedCoordinate trackedCoordinate : coordinateByPath.values()) {
            if (trackedCoordinate.getCoordinatedata() != null) {
                ZkResolver.addEndpoints(
                        trackedCoordinate.getCoordinatedata(),
                        newEndpoints, parameters.getEndpointName());
            }
        }
        return newEndpoints;
    }

    private void notifyClient() {
        synchronized (instanceLock) {
            if (stopped) {
                return;
            }

            // First generate a fresh list of endpoints.
            final List<Endpoint> newEndpoints = getNewEndpoints();

            final Map<String, Endpoint> newEndpointsByName = new HashMap<String, Endpoint>();
            for (final Endpoint endpoint : newEndpoints) {
                newEndpointsByName.put(getEndpointKey(endpoint), endpoint);
            }

            Iterator<Map.Entry<String, Endpoint>> it = clientPicture.entrySet().iterator();
            while (it.hasNext()) {

                final Map.Entry<String, Endpoint> endpointEntry = it.next();
                final String key = endpointEntry.getKey();

                if (! newEndpointsByName.containsKey(key)) {
                    it.remove();
                    clientCallback.endpointEvent(
                            Resolver.ResolverListener.Event.REMOVED_ENDPOINT,
                            endpointEntry.getValue());
                }
            }

            for (final Endpoint endpoint : newEndpoints) {
                String key = getEndpointKey(endpoint);

                if (! clientPicture.containsKey(key)) {
                    clientCallback.endpointEvent(
                            Resolver.ResolverListener.Event.NEW_ENDPOINT, endpoint);
                    clientPicture.put(key, endpoint);
                } else {
                    if (! clientPicture.get(key).equals(endpoint)) {
                        clientCallback.endpointEvent(
                                Resolver.ResolverListener.Event.REMOVED_ENDPOINT,
                                clientPicture.get(key));
                        clientCallback.endpointEvent(
                                Resolver.ResolverListener.Event.NEW_ENDPOINT, endpoint);
                        clientPicture.put(key, endpoint);
                    }
                }
            }

        }
    }

    private void scheduleRefresh(final String path, long delayMillis) {
        // Randomize refreshes to avoid network peaks.
        long delayMillisDistributed = (long) (delayMillis * (0.7 + random.nextDouble() * 0.6));
        synchronized (instanceLock) {
            long now = System.currentTimeMillis();
            if (scheduledRefreshMsByPath.containsKey((path))) {
                long oldSchedule = scheduledRefreshMsByPath.get(path);
                if (oldSchedule < delayMillisDistributed + now) {
                    return;
                }
            }
            scheduledRefreshMsByPath.put(path, delayMillisDistributed + now);
        }
    }

    private void refreshPathWithWatcher(String path) {
        synchronized (instanceLock) {
            TrackedCoordinate trackedCoordinate = coordinateByPath.get(path);
            if (trackedCoordinate == null) {
                // Endpoint has been removed while waiting for refresh.
                return;
            }
            trackedCoordinate.refreshAsync();
        }
    }

}