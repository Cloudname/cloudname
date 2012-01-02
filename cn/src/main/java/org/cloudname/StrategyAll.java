package org.cloudname;

import java.util.List;

/**
 * A strategy that implements "all" and returns everything and does not change order.
 * @author : dybdahl
 */
public class StrategyAll implements ResolverStrategy {

    /**
     * Returns all the endpoints.
     * @param endpoints
     * @return endpoints
     */
    @Override
    public List<Endpoint> filter(List<Endpoint> endpoints) {
        return endpoints;
    }

    /**
     * Doesn't change ordering of endpoints.
     * @param endpoints
     * @return endpoints
     */
    @Override
    public List<Endpoint> order(List<Endpoint> endpoints) {
        return endpoints;
    }

    /**
     * The name of the strategy is "all".
     * @return "all"
     */
    @Override
    public String getName() {
        return "all";
    }

}
