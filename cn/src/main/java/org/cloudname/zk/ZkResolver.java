package org.cloudname.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.cloudname.*;

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
            for (DynamicExpression expression : dynamicAddressesByListener.values()) {
                expression.newZooKeeperInstance(zk);
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
    
    
    static public List<Integer> resolveInstances(Parameters parameters, ZooKeeper zk) throws CloudnameException {
 
        List<Integer> instances = new ArrayList<Integer>();
        if (parameters.getInstance() > -1) {
            instances.add(parameters.getInstance());
        } else {
            try {
                instances = getInstances(zk,
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

        List<Integer> instances = resolveInstances(parameters, zk);
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
    public void addResolverListener(String expression, ResolverListener listener) throws CloudnameException {
        DynamicExpression dynamicExpression = new DynamicExpression(expression, listener);
        dynamicExpression.newZooKeeperInstance(zk);
        synchronized (this) {
            DynamicExpression previousExpression = dynamicAddressesByListener.put(listener, dynamicExpression);
            if (previousExpression != null) {
                throw new IllegalArgumentException("It is not legal to register a listener twice.");
            }
        }
        dynamicExpression.resolve();
    }

    public static void addEndpoints(CoordinateData.Snapshot statusAndEndpoints, List<Endpoint> endpoints, String endpointname) {
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
