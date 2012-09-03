package org.cloudname;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;

/**
 * A strategy that returns the first element of the sorted coordinates (by instance value) hashed with
 * the time of this object creation. This is useful for returning the same endpoint in most cases even
 * if an endpoint is removed or added.
 * @author : dybdahl
 */
public class StrategyAny implements ResolverStrategy {

    // Some systems might not have nano seconds accuracy and we do not want zeros in the least significant
    // numbers.
    private int sortSeed = (int) System.nanoTime() / 1000;
    
    /**
     * Returns a list of the first endpoint if any, else returns the empty list.
     */
    @Override
    public List<Endpoint> filter(List<Endpoint> endpoints) {
        if (endpoints.size() > 0) {
            List<Endpoint> retVal = new ArrayList<Endpoint>();
            retVal.add(endpoints.get(0));
            return retVal;
        }
        // Empty list.
        return endpoints;
    }

    /**
     * We return a list that is sorted differently for different clients. In this way only a few
     * clients are touched when an endpoint is added/removed.
     */
    @Override
    public List<Endpoint> order(List<Endpoint> endpoints) {
        Collections.sort(endpoints, new Comparator<Endpoint>() {
            @Override
            public int compare(Endpoint endpointA, Endpoint endpointB) {
                int instanceA = endpointA.getCoordinate().getInstance() ^ sortSeed;
                int instanceB = endpointB.getCoordinate().getInstance() ^ sortSeed;
                return (instanceA > instanceB ? -1 : (instanceA == instanceB ? 0 : 1));
            }
        });
        return endpoints;
    }

    /**
     * The name of the strategy is "any"
     */
    @Override
    public String getName() {
        return "any";
    }
}
