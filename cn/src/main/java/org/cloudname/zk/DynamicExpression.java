package org.cloudname.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.cloudname.CloudnameException;
import org.cloudname.Endpoint;
import org.cloudname.Resolver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
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
class DynamicExpression implements Watcher, TrackedCoordinate.ExpressionResolverNotify,
        ZkObjectHandler.ConnectionStateChanged {

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
     * A Map with all the coordinate we care about for now.
     */
    final private Map<String, TrackedCoordinate> coordinateByPath =
            new HashMap<String, TrackedCoordinate>();

    
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

    public void start() {
        zkClient.registerListener(this);
        scheduler.scheduleWithFixedDelay(new NodeScanner(""), 1 /* initial delay ms */,
                TIME_BETWEEN_NODE_SCANNING_MS, TimeUnit.MILLISECONDS);
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

    private void scheduleRefresh(String path, long delayMs) {
        try {
            scheduler.schedule(new NodeScanner(path), delayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            if (scheduler.isShutdown()) {
                return;
            }
            log.log(Level.SEVERE, "Got exception while scheduling new refresh", e);
        }
    }

    @Override
    public void connectionUp() {
    }

    @Override
    public void connectionDown() {
    }

    @Override
    public void shutDown() {
       scheduler.shutdown();
    }

    /**
     * The method will try to resolve the expression and find new nodes.
     */
    private class NodeScanner implements Runnable {
        final String path;

        public NodeScanner(final String path) {
            this.path = path;
        }

        @Override
        public void run() {
            if (path.isEmpty()) {
                resolve();
            } else {
                refreshPathWithWatcher(path);
            }
            notifyClient();
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
    public void nodeDead(final String path) {
        synchronized (instanceLock) {
            TrackedCoordinate trackedCoordinate = coordinateByPath.remove(path);
            if (trackedCoordinate == null) {
                return;
            }
            trackedCoordinate.stop();
            // Triggers a new scan, and potential client updates.
            scheduleRefresh("" /** scan for all nodes */, 50 /* ms*/);
        }
    }

    /**
     * Implements interface TrackedCoordinate.ExpressionResolverNotify
     */
    @Override
    public void stateChanged(final String path) {
        // Something happened to a path, schedule a refetch.
        scheduleRefresh(path, 50);
    }

    private void resolve() {
        final List<Endpoint> endpoints;
        try {
            endpoints = zkResolver.resolve(parameters.getExpression());
        } catch (CloudnameException e) {
            log.warning("Exception from cloudname: " + e.toString());
            return;
        }

        final Set<String> validEndpointsPaths = new HashSet<String>();

        for (Endpoint endpoint : endpoints) {

            final String statusPath = ZkCoordinatePath.getStatusPath(endpoint.getCoordinate());
            validEndpointsPaths.add(statusPath);

            final TrackedCoordinate trackedCoordinate;

            synchronized (instanceLock) {

                // If already discovered, do nothing.
                if (coordinateByPath.containsKey(statusPath)) {
                    continue;
                }
                trackedCoordinate = new TrackedCoordinate(this, statusPath, zkClient);
                coordinateByPath.put(statusPath, trackedCoordinate);
            }
            // Tracked coordinate has to be in coordinateByPath before start is called, or events
            // gets lost.
            trackedCoordinate.start();
            try {
                trackedCoordinate.waitForFirstData();
            } catch (InterruptedException e) {
                log.log(Level.SEVERE, "Got interrupt while waiting for data.", e);
                return;
            }
        }

         // Remove tracked coordinates that does not resolve.
        synchronized (instanceLock) {
            for (Iterator<Map.Entry<String, TrackedCoordinate> > it =
                         coordinateByPath.entrySet().iterator();
                 it.hasNext(); /* nop */) {
                Map.Entry<String, TrackedCoordinate> entry = it.next();

                if (! validEndpointsPaths.contains(entry.getKey())) {
                    log.info("Killing endpoint " + entry.getKey() + ": No longer resolved.");
                    entry.getValue().stop();
                    it.remove();
                }
            }
        }
    }

    private String getEndpointKey(final Endpoint endpoint) {
        return endpoint.getCoordinate().asString() + "@" + endpoint.getName();
    }


    private List<Endpoint> getNewEndpoints() {
        final List<Endpoint> newEndpoints = new ArrayList<Endpoint>();
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
        }
            // First generate a fresh list of endpoints.
        final List<Endpoint> newEndpoints = getNewEndpoints();

        synchronized (instanceLock) {
            final Map<String, Endpoint> newEndpointsByName = new HashMap<String, Endpoint>();
            for (final Endpoint endpoint : newEndpoints) {
                newEndpointsByName.put(getEndpointKey(endpoint), endpoint);
            }
            final Iterator<Map.Entry<String, Endpoint>> it = clientPicture.entrySet().iterator();
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
                final String key = getEndpointKey(endpoint);

                if (! clientPicture.containsKey(key)) {
                    clientCallback.endpointEvent(
                            Resolver.ResolverListener.Event.NEW_ENDPOINT, endpoint);
                    clientPicture.put(key, endpoint);
                    continue;
                }
                final Endpoint clientEndpoint = clientPicture.get(key);
                if (endpoint.equals(clientEndpoint)) { continue; }
                if (endpoint.getHost().equals(clientEndpoint.getHost()) &&
                        endpoint.getName().equals(clientEndpoint.getName()) &&
                        endpoint.getPort() == clientEndpoint.getPort() &&
                        endpoint.getProtocol().equals(clientEndpoint.getProtocol())) {
                    clientCallback.endpointEvent(
                            Resolver.ResolverListener.Event.MODIFIED_ENDPOINT_DATA, endpoint);
                    clientPicture.put(key, endpoint);
                    continue;
                }
                clientCallback.endpointEvent(
                        Resolver.ResolverListener.Event.REMOVED_ENDPOINT,
                        clientPicture.get(key));
                clientCallback.endpointEvent(
                        Resolver.ResolverListener.Event.NEW_ENDPOINT, endpoint);
                clientPicture.put(key, endpoint);
            }
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