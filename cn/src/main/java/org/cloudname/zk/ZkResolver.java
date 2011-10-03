package org.cloudname.zk;

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

    /**
     * Resolve address and return a single endpoint.  If the
     * resolution strategy results in multiple endpoints, we return
     * the first endpoint from a ranked list of endpoints.
     */
    @Override
    public Endpoint resolve(String address) {
        List<Endpoint> endpoints = resolveAll(address);
        if (endpoints.size() == 0) {
            return null;
        }

        return endpoints.get(0);
    }

    /**
     * TODO(borud): implement.
     */
    @Override
    public List<Endpoint> resolveAll(String address) {
        return Collections.emptyList();
    }
}