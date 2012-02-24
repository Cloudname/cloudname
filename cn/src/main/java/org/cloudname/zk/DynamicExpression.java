package org.cloudname.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.cloudname.CloudnameException;
import org.cloudname.Endpoint;
import org.cloudname.Resolver;

import java.util.*;




class DynamicExpression implements Watcher, TrackedCoordinate.ExpressionResolverNotify, ZkUserInterface {

    final private Map<String, Endpoint> clientPicture = new HashMap<String, Endpoint>();
    final private Resolver.ResolverListener clientCallback;
    final private Map<String, Long> dirtyTimeMap = new HashMap<String, Long>();
    final private ZkResolver.Parameters parameters;

    final private Map<String, TrackedCoordinate> coordinateByPath =
            new HashMap<String, TrackedCoordinate>();

    final private Random random = new Random();
    private long lastResolve = 0;
    private boolean stopped = false;
    private ZooKeeper zk = null;

    public DynamicExpression(String expression, Resolver.ResolverListener clientCallback) {
        this.clientCallback = clientCallback;
        this.parameters = new ZkResolver.Parameters(expression);
    }

    public void stop() {
        synchronized (this) {
            stopped = true;
            coordinateByPath.clear();
        }
    }

    public void resolve() throws CloudnameException {
        List<Integer> instances = null;

        synchronized (this) {
            instances = ZkResolver.resolveInstances(parameters, zk);
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
            lastResolve = System.currentTimeMillis();
        }
        notifyClient();
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
                ZkResolver.addEndpoints(trackedCoordinate.getCoordinatedata(), newEndpoints, parameters.getEndpointName());
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
                    if (! clientPicture.get(key).equals(endpoint)) {
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
        delayMillis = (long) (delayMillis * (0.7 + random.nextDouble() * 0.6));
        synchronized (this) {
            long now = System.currentTimeMillis();
            if (dirtyTimeMap.containsKey((path))) {
                long oldSchedule = dirtyTimeMap.get(path);
                if (oldSchedule < delayMillis + now) {
                    return;
                }
            }
            dirtyTimeMap.put(path, delayMillis + now);
        }
    }


    private boolean timeToReresolve() {
        synchronized (this) {
            return lastResolve + 1 * 60 * 1000 < System.currentTimeMillis();
        }
    }

    public void wakeUp() {
        if (timeToReresolve()) {
            try {
                resolve();
            } catch (CloudnameException e) {
              //  e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        List<String> paths = new ArrayList<String>();
        synchronized (this) {
            Long now = System.currentTimeMillis();  // msec
            for (Map.Entry<String, Long> entry : dirtyTimeMap.entrySet()) {
                if (now - entry.getValue()  >  0) {
                    paths.add(entry.getKey());
                }
            }
            for (String path : paths) {
                dirtyTimeMap.remove(path);
            }
        }
        for (String path : paths) {
            refreshPathWithWatcher(path);
        }
    }

    private void refreshPathWithWatcher(String path) {
        TrackedCoordinate e = coordinateByPath.get(path);
        if (e == null) {
            // Endpoint has been removed while waiting for refresh.
            return;
        }
        e.newZooKeeperInstance(zk);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        synchronized (this) {
            if (stopped) {

            }
        }
        String path = watchedEvent.getPath();
        Event.KeeperState state = watchedEvent.getState();
        Event.EventType type = watchedEvent.getType();

        //log.fine("Dynamic watch got event with path " + path + " state " + state.name() + " type " + type.name());

        switch (state) {
            case Expired:
            case AuthFailed:
            case Disconnected:
                // Try again in 10 secs
                scheduleRefresh(path, 10000);
                break;
        }
        switch (type) {
            case NodeChildrenChanged:
            case None:
            case NodeCreated:
                //log.fine("Unexpected event from zookeeper, path: " + path + " event " +
                //        type.name() + watchedEvent.toString());
                scheduleRefresh(path, 2000);

                break;
            case NodeDeleted:
                synchronized (this) {
                    coordinateByPath.remove(path);
                    notifyClient();
                    dirtyTimeMap.remove(path);
                    return;
                }
            case NodeDataChanged:
                refreshPathWithWatcher(path);
                scheduleRefresh(path, 10 * 60 * 1000);  // 10 mins
                break;
        }

    }

    @Override
    public void stateChanged() {
        notifyClient();
    }

    @Override
    public void zooKeeperDown() {

    }

    @Override
    public void newZooKeeperInstance(ZooKeeper zk) {
        this.zk = zk;
    }

    @Override
    public void timeEvent() {

    }
}