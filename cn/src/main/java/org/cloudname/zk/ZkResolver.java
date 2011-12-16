package org.cloudname.zk;

import com.sun.java.swing.action.AlignLeftAction;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.cloudname.CloudnameException;
import org.cloudname.Resolver;
import org.cloudname.Coordinate;
import org.cloudname.Endpoint;

import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


/**
 * This class is used to resolve Cloudname coordinates into endpoints.
 *
 * @author borud
 */
public class ZkResolver implements Resolver {
    // Included here for convenience.  Matches a bare coordinate.
    public static final Pattern coordinatePattern = Coordinate.coordinatePattern;
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
    enum Strategy {
        ONE_INSTANCE,
        ANY_INSTANCE,
        ALL_INSTANCES
    };
    Strategy strategy = null;

    private boolean trySetEndPointPattern(String address) {
        Matcher m = strategyPattern.matcher(address);
        if (! m.matches()) {
            return false;
        }
        endpointName = m.group(1);
        instance = Integer.parseInt(m.group(2));
        strategy = Strategy.ONE_INSTANCE;
        service = m.group(3);
        user = m.group(4);
        cell = m.group(5);
        return true;

    }

    private Strategy getStrategy(String strategyString) {
        if (strategyString.compareToIgnoreCase("any") == 0 /* equal */) {
            return Strategy.ANY_INSTANCE;
        }
        if (strategyString.compareToIgnoreCase("all") == 0 /* equal */) {
            return Strategy.ALL_INSTANCES;
        }
        throw new IllegalStateException("Unknown strategy:" + strategyString);                 
    }

    private boolean trySetStrategyPattern(String address) {
        Matcher m = strategyPattern.matcher(address);
        if (! m.matches()) {
            return false;
        }
        strategy = getStrategy(m.group(1));
        service = m.group(2);
        user = m.group(3);
        cell = m.group(4);
        return true;
    }

    private boolean trySetEndpointStrategyPattern(String address) {
        Matcher m = endpointStrategyPattern.matcher(address);
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
    
    /**
     * TODO(borud): implement.
     */
    @Override
    public List<Endpoint> resolve(String address) {
        log.info("Resolving " + address);
        // Verify that address is recognized.
        if (! (trySetEndPointPattern(address) ||
              trySetStrategyPattern(address) ||
              trySetEndpointStrategyPattern(address))) {
            throw new IllegalStateException("Could not parse address:" + address);
        }

        // Based on address, generate list of paths.
        List<Integer> instances = new ArrayList<Integer>();
        if (strategy == Strategy.ONE_INSTANCE) {
            instances.add(instance);
        } else {
            instances = getInstances(ZkCoordinatePath.coordinateAsPath(cell, user, service));
        }
        List<Endpoint> endpoints = new ArrayList<Endpoint>();
        for (Integer instance : instances) {
            String path = ZkCoordinatePath.coordinateAsPath(cell, user, service, instance);
            ZkStatusEndpoint statusEndpoint = new ZkStatusEndpoint(zk, path);
            statusEndpoint.load();
            if (endpointName == null) {
                statusEndpoint.addAllEndpoints(endpoints);
            } else {
                endpoints.add(statusEndpoint.getEndpoint(endpointName));
            }
        }
        return endpoints;
    }
    private ZooKeeper zk;
    public ZkResolver(ZooKeeper zk) {
        this.zk = zk;
    }
}