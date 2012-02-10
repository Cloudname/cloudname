package org.cloudname;

import java.util.ArrayList;
import java.util.List;

/**
 * A strategy that returns the first endpoint in the list.
 * TODO(dybdahl): Extend this strategy to pick a random element based on this machines name, process id
 * or something.
 * @author : dybdahl
 */
public class StrategyAny implements ResolverStrategy {

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
     * Does not change the ordering, after all it is 0 or 1 element in the list.
     */
    @Override
    public List<Endpoint> order(List<Endpoint> endpoints) {
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
