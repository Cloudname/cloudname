package org.cloudname.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.cloudname.*;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


/**
 * This class is used to resolve Cloudname coordinates into endpoints.
 *
 * @author borud
 */
public class ZkResolver implements Resolver, ZkUserInterface {

    private static final Logger log = Logger.getLogger(ZkResolver.class.getName());

    private ZooKeeper zk = null;

    private Map<String, ResolverStrategy> strategies;

    @Override
    public void zooKeeperDown() {
        synchronized (this) {
            this.zk = null;
            for (ResolverListener listener : dynamicAddressesByListener.keySet()) {
                listener.endpointEvent(ResolverListener.Event.LOST_CONNECTION, null);
            }
        }
    }

    @Override
    public void newZooKeeperInstance(ZooKeeper zk) {
        log.fine("ZkResolver, new zeekeeper instance.");
        synchronized (this) {
            this.zk = zk;
            for (ResolverListener listener : dynamicAddressesByListener.keySet()) {
                listener.endpointEvent(ResolverListener.Event.CONNECTION_OK, null);
            }
        }
    }


    Map<ResolverListener, DynamicExpression> dynamicAddressesByListener = new HashMap<ResolverListener, DynamicExpression>();
    
    @Override
    public void timeEvent() {
        for (DynamicExpression dynamicExpression : dynamicAddressesByListener.values()) {
            dynamicExpression.wakeUp();
        }     
    }

    private ZooKeeper getZooKeeper() throws CloudnameException {
        synchronized (this) {
            if (zk == null) {
                throw new CloudnameException("Connection to ZooKeeper is down.");
            }
            return zk;
        }
    }
            
    
    public static class Builder {

        Map<String, ResolverStrategy> strategies = new HashMap<String, ResolverStrategy>();

        public Builder addStrategy(ResolverStrategy strategy) {
            strategies.put(strategy.getName(), strategy);
            return this;
        }

        public Map<String, ResolverStrategy> getStrategies() {
            return strategies;
        }
        
        public ZkResolver build() {
            return new ZkResolver(this);
        }

    }
    
    
    // Matches coordinate with endpoint of the form:
    // endpoint.instance.service.user.cell
    public static final Pattern endpointPattern
        = Pattern.compile( "^([a-z][a-z0-9-_]*)\\." // endpoint
                         + "(\\d+)\\." // instance
                         + "([a-z][a-z0-9-_]*)\\." // service
                         + "([a-z][a-z0-9-_]*)\\." // user
                         + "([a-z][a-z-_]*)\\z"); // cell

    // Parses abstract coordinate of the form:
    // strategy.service.user.cell.  This pattern is useful for
    // resolving hosts, but not endpoints.
    public static final Pattern strategyPattern
        = Pattern.compile( "^([a-z][a-z0-9-_]*)\\." // strategy
                         + "([a-z][a-z0-9-_]*)\\." // service
                         + "([a-z][a-z0-9-_]*)\\." // user
                         + "([a-z][a-z0-9-_]*)\\z"); // cell

    // Parses abstract addresses of the form:
    // endpoint.strategy.service.user.cell.
    public static final Pattern endpointStrategyPattern
        = Pattern.compile( "^([a-z][a-z0-9-_]*)\\." // endpoint
                         + "([a-z][a-z0-9-_]*)\\." // strategy
                         + "([a-z][a-z0-9-_]*)\\." // service
                         + "([a-z][a-z0-9-_]*)\\." // user
                         + "([a-z][a-z0-9-_]*)\\z"); // cell


    /**
     * Inner class to keep track of parameters parsed from addressExpression.
     */
    public static class Parameters {
        private String endpointName = null;
        private Integer instance = null;
        private String service = null;
        private String user = null;
        private String cell = null;
        private String strategy = null;

        /**
         * Constructor that takes an addressExperssion and sets the inner variables.
         * @param addressExpression
         */
        public Parameters(String addressExpression) {
            if (! (trySetEndPointPattern(addressExpression) ||
                    trySetStrategyPattern(addressExpression) ||
                    trySetEndpointStrategyPattern(addressExpression))) {
                throw new IllegalStateException("Could not parse addressExpression:" + addressExpression);
            }
 
        }

        /**
         * Returns strategy.
         * @return the string (e.g. "all" or "any", or "" if there is no strategy (but instance is specified).
         */
        public String getStrategy() {
            return strategy;
        }

        /**
         * Returns endpoint name if set or "" if not set.
         * @return endpointname.
         */
        public String getEndpointName() {
            return endpointName;
        }

        /**
         * Returns instance if set or negative number if not set.
         * @return instance number.
         */
        public Integer getInstance() {
            return instance;
        }

        /**
         * Returns service
         * @return  service name.
         */
        public String getService() {
            return service;
        }

        /**
         * Returns user
         * @return user.
         */
        public String getUser() {
            return user;
        }

        /**
         * Returns cell.
         * @return cell.
         */
        public String getCell() {
            return cell;
        }

        private boolean trySetEndPointPattern(String addressExperssion) {
            Matcher m = endpointPattern.matcher(addressExperssion);
            if (! m.matches()) {
                return false;
            }
            endpointName = m.group(1);
            instance = Integer.parseInt(m.group(2));
            strategy = "";
            service = m.group(3);
            user = m.group(4);
            cell = m.group(5);
            return true;

        }

        private boolean trySetStrategyPattern(String addressExpression) {
            Matcher m = strategyPattern.matcher(addressExpression);
            if (! m.matches()) {
                return false;
            }
            endpointName = "";
            strategy = m.group(1);
            service = m.group(2);
            user = m.group(3);
            cell = m.group(4);
            instance = -1;
            return true;
        }

        private boolean trySetEndpointStrategyPattern(String addressExperssion) {
            Matcher m = endpointStrategyPattern.matcher(addressExperssion);
            if (! m.matches()) {
                return false;
            }
            endpointName = m.group(1);
            strategy = m.group(2);
            service = m.group(3);
            user = m.group(4);
            cell = m.group(5);
            instance = -1;
            return true;
        }

    }

    /**
     * Constructor, to be called from the inner Dynamic class.
     * @param builder
     */
    private ZkResolver(Builder builder) {
        this.strategies = builder.getStrategies();
    }
    
    
    private List<Integer> resolveInstances(Parameters parameters) throws CloudnameException {
 
        List<Integer> instances = new ArrayList<Integer>();
        if (parameters.getInstance() > -1) {
            instances.add(parameters.getInstance());
        } else {
            try {
                instances = getInstances(getZooKeeper(),
                        ZkCoordinatePath.coordinateWithoutInstanceAsPath(parameters.getCell(),
                        parameters.getUser(), parameters.getService()));
            } catch (InterruptedException e) {
                throw new CloudnameException(e);
            }
        }
        return instances;
    }

    
    @Override
    public List<Endpoint> resolve(String addressExpression) throws CloudnameException {
        Parameters parameters = new Parameters(addressExpression);

        List<Integer> instances = resolveInstances(parameters);
        List<Endpoint> endpoints = new ArrayList<Endpoint>();
        for (Integer instance : instances) {
            String statusPath = ZkCoordinatePath.getStatusPath(parameters.getCell(), parameters.getUser(),
                    parameters.getService(), instance);

            try {
                if (! Util.exist(getZooKeeper(), statusPath)) {
                    continue;
                }
            } catch (InterruptedException e) {
                throw new CloudnameException(e);

            }
            CoordinateData coordinateData = CoordinateData.loadCoordianteData(statusPath, getZooKeeper(), null);
            addEndpoints(coordinateData.snapshot(), endpoints, parameters.getEndpointName());

        }
        if (parameters.getStrategy().equals("")) {
          return endpoints;
        }
        ResolverStrategy strategy = strategies.get(parameters.getStrategy());
        return strategy.order(strategy.filter(endpoints));
    }



    @Override
    public void removeResolverListener(ResolverListener listener) {
        synchronized (this) {
            DynamicExpression expression = dynamicAddressesByListener.remove(listener);
            if (expression == null) {
                throw new IllegalArgumentException("Do not have the listener in my list.");
            }
            expression.stop();
        }
        log.fine("Removed listener.");
    }

    @Override
    public void addResolverListener(String expression, ResolverListener listener) {
        DynamicExpression dynamicExpression = new DynamicExpression(expression, listener);
        synchronized (this) {
            DynamicExpression previousExpression = dynamicAddressesByListener.put(listener, dynamicExpression);
            if (previousExpression != null) {
                throw new IllegalArgumentException("It is not legal to register a listener twice.");
            }
        }
        dynamicExpression.resolve();
    }

    static private void addEndpoints(CoordinateData.Snapshot  statusAndEndpoints, List<Endpoint> endpoints, String endpointname) {
        if (statusAndEndpoints.getServiceStatus().getState() != ServiceState.RUNNING) {
            return;
        }
        if (endpointname.equals("")) {
            statusAndEndpoints.appendAllEndpoints(endpoints);
        } else {
            Endpoint e =  statusAndEndpoints.getEndpoint(endpointname);
            if (e != null) {
                endpoints.add(e);
            }
        }
    }

    public class DynamicExpression implements Watcher, ExpressionResolver.ExpressionResolverNotify {

        final private Map<String, Endpoint> clientPicture = new HashMap<String, Endpoint>();
        final private ResolverListener clientCallback;
        final private Map<String, Long> dirtyTimeMap = new HashMap<String, Long>();
        final private Parameters parameters;

        final private Map<String, ExpressionResolver> expressionResolverByExpression =
                new HashMap<String, ExpressionResolver>();

        final private Random random = new Random();
        private long lastResolve = 0;
        private boolean stopped = false;

        public DynamicExpression(String expression, ResolverListener clientCallback) {
            this.clientCallback = clientCallback;
            this.parameters = new Parameters(expression);
        }

        public void stop() {
            synchronized (this) {
                stopped = true;
                expressionResolverByExpression.clear();
            }
        }

        public void resolve() {
            List<Integer> instances = null;
            try {
                instances = resolveInstances(parameters);
            } catch (CloudnameException e) {
                log.fine("Got cloudname exception " + e.getMessage());
                return;
            }
            Map<String, ExpressionResolver> tempStatusAndEndpointsMap =
                    new HashMap<String, ExpressionResolver>();

            for (Integer instance : instances) {
                String statusPath = ZkCoordinatePath.getStatusPath(parameters.getCell(), parameters.getUser(),
                        parameters.getService(), instance);


                ExpressionResolver expressionResolver = new ExpressionResolver(this, statusPath);
                expressionResolver.newZooKeeperInstance(zk);
                tempStatusAndEndpointsMap.put(statusPath, expressionResolver);
            }


            synchronized (this) {
                expressionResolverByExpression.clear();
                expressionResolverByExpression.putAll(tempStatusAndEndpointsMap);
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
                    log.fine("Someone called me, but I am stopped. No worries.");
                    return;
                }
            }
            // First generate a fresh list of endpoints.
            List<Endpoint> newEndpoints = new ArrayList<Endpoint>();
            synchronized (this) {
                for (ExpressionResolver expressionResolver : expressionResolverByExpression.values()) {
                    addEndpoints(expressionResolver.getCoordinatedata(), newEndpoints, parameters.getEndpointName());
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
                                ResolverListener.Event.REMOVED_ENDPOINT, endpointEntry.getValue());
                    }
                }

                for (Endpoint endpoint : newEndpoints) {
                    String key = getEndpointKey(endpoint);

                    if (! clientPicture.containsKey(key)) {
                        clientCallback.endpointEvent(
                                ResolverListener.Event.NEW_ENDPOINT, endpoint);
                        clientPicture.put(key, endpoint);
                    } else {
                        if (! clientPicture.get(key).equals(endpoint)) {
                            clientCallback.endpointEvent(
                                    ResolverListener.Event.REMOVED_ENDPOINT, clientPicture.get(key));
                            clientCallback.endpointEvent(
                                    ResolverListener.Event.NEW_ENDPOINT, endpoint);
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

        private void wakeUp() {
            if (timeToReresolve()) {
                resolve();
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
            ExpressionResolver e = expressionResolverByExpression.get(path);
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
                    log.fine("Got callback from ZooKeeper on a dead listener. No worries.");
                    return;
            }
            }
            String path = watchedEvent.getPath();
            Event.KeeperState state = watchedEvent.getState();
            Event.EventType type = watchedEvent.getType();

            log.fine("Dynamic watch got event with path " + path + " state " + state.name() + " type " + type.name());
            
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
                    log.fine("Unexpected event from zookeeper, path: " + path + " event " +
                            type.name() + watchedEvent.toString());
                    scheduleRefresh(path, 2000);

                    break;
                case NodeDeleted:
                    synchronized (this) {
                        expressionResolverByExpression.remove(path);
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
    }
    
    static private List<Integer> getInstances(ZooKeeper zk, String path) throws CloudnameException, InterruptedException {
        List<Integer> paths = new ArrayList<Integer>();
        try {
            List<String> children = zk.getChildren(path, false /* watcher */);
            for (String child : children) {
                paths.add(Integer.parseInt(child));
            }
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        }
        return paths;
    }
}
