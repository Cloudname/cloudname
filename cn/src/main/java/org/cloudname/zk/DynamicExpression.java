package org.cloudname.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.cloudname.CloudnameException;
import org.cloudname.Endpoint;
import org.cloudname.Resolver;

import java.util.*;
import java.util.logging.Logger;

/**
 * Class that is capable of tracking an expression. An expression can include many nodes.
 * The number of nodes is dynamic and can change over time.
 * For now, the implementation is rather simple. For single endpoints it does use feedback from ZooKeeper
 * watcher events. For keeping track of new nodes, it does a scan on regular intervals.
 * @author dybdahl
 */
class DynamicExpression implements Watcher, TrackedCoordinate.ExpressionResolverNotify, ZkUserInterface {

    /**
     * Keeps track of what picture (what an expression has resolved to) is sent to the user so that
     * we know when to send new events.
     */
    final private Map<String, Endpoint> clientPicture = new HashMap<String, Endpoint>();

    /**
     * Where to notify changes.
     */
    final private Resolver.ResolverListener clientCallback;

    /**
     * A path can be scheduled to be read later, i.e. if we think there are changes, we got error etc.
     */
    final private Map<String, Long> scheduledRefreshMsByPath = new HashMap<String, Long>();

    /**
     * This is the expression to dynamically resolved represented as ZkResolver.Parameters.
     */
    final private ZkResolver.Parameters parameters;

    /**
     * When ZooKeeper reports an error about an path, when to try to read it again.
     */
    final private int RETRY_INTERVAL_ZOOKEEPER_ERROR_MS = 30000;      // 30 seconds

    /**
     * We wait a bit after a node has changed because in many cases there might be several updates, e.g. an
     * application registers several endpoints, each causing an update.
     */
    final private int REFRESH_NODE_AFTER_CHANGE_MS = 2000;            // two seconds

    /**
     * Does a full scan with this interval.
     */
    final private int TIME_BETWEEN_NODE_SCANNING_MS = 1 * 60 * 1000;  // one minute

    /**
     * A Map with all the coordinate we care about for now.
     */
    final private Map<String, TrackedCoordinate> coordinateByPath =
            new HashMap<String, TrackedCoordinate>();

    /**
     * We always add some random noise to when to do things so not all servers fire at the same time against
     * ZooKeeper.
     */
    final private Random random = new Random();
    
    private static final Logger log = Logger.getLogger(DynamicExpression.class.getName());

    /**
     * The last time we did a full scan with the expression.
     */
    private long lastResolveMs = 0;

    private boolean stopped = false;
    private ZooKeeper zk = null;

    /**
     * Start getting notified about changes to expression.
     * @param expression Coordinate expression.
     * @param clientCallback called on changes and initially.
     */
    public DynamicExpression(String expression, Resolver.ResolverListener clientCallback) {
        this.clientCallback = clientCallback;
        this.parameters = new ZkResolver.Parameters(expression);
    }

    /**
     * Stop receiving callbacks about coordinate.
     */
    public void stop() {
        synchronized (this) {
            stopped = true;
            coordinateByPath.clear();
        }
    }

    private List<String> pullPathsToBeRefreshed() {
        List<String> pathsToRefresh = new ArrayList<String>();
        synchronized (this) {
            Long nowMillis = System.currentTimeMillis();
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

    /**
     * Method  from ZkUserInterface.
     * The method will try to resolve the expression from time to time and check if there are paths that are
     * scheduled for refresh.
     */
    @Override
    public void timeEvent() {
        // Is it time to check for new nodes?
        if (timeToReresolve()) {
            resolve();
            notifyClient();
            synchronized (this) {
                lastResolveMs = System.currentTimeMillis();
            }
        }
        // Periodically check every node for changes.
        List<String>  pathsToRefresh = pullPathsToBeRefreshed();
        for (String path : pathsToRefresh) {
            refreshPathWithWatcher(path);
        }
    }

    /**
     * Callback from zookeeper watcher.
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        synchronized (this) {
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
                synchronized (this) {
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

    /**
     * Method  from ZkUserInterface.
     */
    @Override
    public void zooKeeperDown() {

    }

    /**
     * Method  from ZkUserInterface.
     */
    @Override
    public void newZooKeeperInstance(ZooKeeper zk) {
        this.zk = zk;
    }

    private void resolve() {
        List<Integer> instances = null;

        synchronized (this) {
            try {
                instances = ZkResolver.resolveInstances(parameters, zk);
            } catch (CloudnameException e) {
                log.warning("Exception from cloudname: " + e.toString());
                return;
            }
        }

        Map<String, TrackedCoordinate> tempStatusAndEndpointsMap =
                new HashMap<String, TrackedCoordinate>();

        for (Integer instance : instances) {
            String statusPath = ZkCoordinatePath.getStatusPath(parameters.getCell(), parameters.getUser(),
                    parameters.getService(), instance);


            TrackedCoordinate trackedCoordinate = new TrackedCoordinate(this, statusPath);
            trackedCoordinate.newZooKeeperInstance(zk);
            tempStatusAndEndpointsMap.put(statusPath, trackedCoordinate);
        }


        synchronized (this) {
            coordinateByPath.clear();
            coordinateByPath.putAll(tempStatusAndEndpointsMap);
            lastResolveMs = System.currentTimeMillis();
        }
    }

    private String getEndpointKey(Endpoint endpoint) {
        return endpoint.getCoordinate().asString() + "@" + endpoint.getName();
    }

    private void notifyClient() {
        synchronized (this) {
            if (stopped) {
                return;
            }
        }
        // First generate a fresh list of endpoints.
        List<Endpoint> newEndpoints = new ArrayList<Endpoint>();
        synchronized (this) {
            for (TrackedCoordinate trackedCoordinate : coordinateByPath.values()) {
                ZkResolver.addEndpoints(
                        trackedCoordinate.getCoordinatedata(), newEndpoints, parameters.getEndpointName());
            }

            Map<String, Endpoint> newEndpointsByName = new HashMap<String, Endpoint>();
            for (Endpoint endpoint : newEndpoints) {
                newEndpointsByName.put(getEndpointKey(endpoint), endpoint);
            }

            for (Map.Entry<String, Endpoint> endpointEntry : clientPicture.entrySet()) {
                String key = endpointEntry.getKey();

                if (! newEndpointsByName.containsKey(key)) {
                    clientPicture.remove(key);
                    clientCallback.endpointEvent(
                            Resolver.ResolverListener.Event.REMOVED_ENDPOINT, endpointEntry.getValue());
                }
            }

            for (Endpoint endpoint : newEndpoints) {
                String key = getEndpointKey(endpoint);

                if (! clientPicture.containsKey(key)) {
                    clientCallback.endpointEvent(
                            Resolver.ResolverListener.Event.NEW_ENDPOINT, endpoint);
                    clientPicture.put(key, endpoint);
                } else {
                    if (! clientPicture.get(key).equalsEndpoint(endpoint)) {
                        clientCallback.endpointEvent(
                                Resolver.ResolverListener.Event.REMOVED_ENDPOINT, clientPicture.get(key));
                        clientCallback.endpointEvent(
                                Resolver.ResolverListener.Event.NEW_ENDPOINT, endpoint);
                        clientPicture.put(key, endpoint);
                    }
                }
            }

        }
    }

    private void scheduleRefresh(String path, long delayMillis) {
        // Randomize refreshes to avoid network peaks.
        long delayMillisDistributed = (long) (delayMillis * (0.7 + random.nextDouble() * 0.6));
        synchronized (this) {
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

    private boolean timeToReresolve() {
        synchronized (this) {
            return lastResolveMs + TIME_BETWEEN_NODE_SCANNING_MS < System.currentTimeMillis();
        }
    }

    private void refreshPathWithWatcher(String path) {
        synchronized (this) {
            TrackedCoordinate trackedCoordinate = coordinateByPath.get(path);
            if (trackedCoordinate == null) {
                // Endpoint has been removed while waiting for refresh.
                return;
            }
            trackedCoordinate.newZooKeeperInstance(zk);
        }
    }
}