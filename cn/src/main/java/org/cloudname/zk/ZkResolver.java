package org.cloudname.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.cloudname.CloudnameException;
import org.cloudname.Resolver;
import org.cloudname.Endpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


/**
 * This class is used to resolve Cloudname coordinates into endpoints.
 *
 * @author borud
 */
public class ZkResolver implements Resolver {

    enum InstanceStrategy {
        ONE_INSTANCE,
        ANY_INSTANCE,
        ALL_INSTANCES
    }

    private static final Logger log = Logger.getLogger(ZkResolver.class.getName());

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

    private String endpointName = null;
    private Integer instance = null;
    private String service = null;
    private String user = null;
    private String cell = null;
    InstanceStrategy strategy = null;
    private ZooKeeper zk;

    /**
     * Constructor
     * @param zk  ZooKeeper that is used for resolving.
     */
    public ZkResolver(ZooKeeper zk) {
        this.zk = zk;
    }

    @Override
    public List<Endpoint> resolve(String addressExperssion) {
        log.info("Resolving " + addressExperssion);
        // Verify that addressExperssion is recognized.
        if (! (trySetEndPointPattern(addressExperssion) ||
                trySetStrategyPattern(addressExperssion) ||
                trySetEndpointStrategyPattern(addressExperssion))) {
            throw new IllegalStateException("Could not parse addressExperssion:" + addressExperssion);
        }

        // Based on addressExperssion, generate list of paths.
        List<Integer> instances = new ArrayList<Integer>();
        if (strategy == InstanceStrategy.ONE_INSTANCE) {
            instances.add(instance);
        } else {
            instances = getInstances(ZkCoordinatePath.coordinateWithoutInstanceAsPath(cell, user, service));
        }
        List<Endpoint> endpoints = new ArrayList<Endpoint>();
        for (Integer instance : instances) {
            String path = ZkCoordinatePath.getStatusPath(cell, user, service, instance);
            ZkStatusAndEndpoints statusAndEndpoints = new ZkStatusAndEndpoints.Builder(zk, path).load().build();
            if (endpointName == null) {
                statusAndEndpoints.returnAllEndpoints(endpoints);
            } else {
                endpoints.add(statusAndEndpoints.getEndpoint(endpointName));
            }
        }
        return endpoints;
    }

    private boolean trySetEndPointPattern(String addressExperssion) {
        Matcher m = endpointPattern.matcher(addressExperssion);
        if (! m.matches()) {
            return false;
        }
        endpointName = m.group(1);
        instance = Integer.parseInt(m.group(2));
        strategy = InstanceStrategy.ONE_INSTANCE;
        service = m.group(3);
        user = m.group(4);
        cell = m.group(5);
        return true;

    }

    private InstanceStrategy getStrategy(String strategyString) {
        if (strategyString.compareToIgnoreCase("any") == 0 /* equals any */) {
            return InstanceStrategy.ANY_INSTANCE;
        }
        if (strategyString.compareToIgnoreCase("all") == 0 /* equal all */) {
            return InstanceStrategy.ALL_INSTANCES;
        }
        throw new IllegalStateException("Unknown strategy:" + strategyString);
    }

    private boolean trySetStrategyPattern(String addressExpression) {
        Matcher m = strategyPattern.matcher(addressExpression);
        if (! m.matches()) {
            return false;
        }
        strategy = getStrategy(m.group(1));
        service = m.group(2);
        user = m.group(3);
        cell = m.group(4);
        return true;
    }

    private boolean trySetEndpointStrategyPattern(String addressExperssion) {
        Matcher m = endpointStrategyPattern.matcher(addressExperssion);
        if (! m.matches()) {
            return false;
        }
        endpointName = m.group(1);
        strategy = getStrategy(m.group(2));
        service = m.group(3);
        user = m.group(4);
        cell = m.group(5);
        return true;
    }

    private List<Integer> getInstances(String path) {
        List<Integer> paths = new ArrayList<Integer>();
        try {
            List<String> children = zk.getChildren(path, false /* watcher */);
            for (String child : children) {
                paths.add(Integer.parseInt(child));
            }
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
        return paths;
    }
}

