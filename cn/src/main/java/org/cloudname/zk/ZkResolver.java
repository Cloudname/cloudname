package org.cloudname.zk;

import com.sun.java.swing.action.AlignLeftAction;
import org.cloudname.Resolver;
import org.cloudname.Coordinate;
import org.cloudname.Endpoint;

import java.util.Collections;
import java.util.List;
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
     
    /**
     * TODO(borud): implement.
     */
    @Override
    public List<Endpoint> resolve(String address) {
        if (! (trySetEndPointPattern(address) ||
              trySetStrategyPattern(address) ||
              trySetEndpointStrategyPattern(address))) {
            throw new IllegalStateException("Could not parse address:" + address);
        }
        String path;
        if (strategy == Strategy.ONE_INSTANCE) {
            path = Util.coordinateAsPath(cell, user, service, instance);
        } else {
            path = Util.coordinateAsPath(cell, user, service);
        }

        return Collections.emptyList();
    }
}