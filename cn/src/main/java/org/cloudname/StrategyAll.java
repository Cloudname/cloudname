package org.cloudname;

import java.util.List;

/**
 * A strategy that implements "all" and returns everything and does not change order.
 * @author : dybdahl
 */
public class StrategyAll implements ResolverStrategy {
    @Override
    public List<Endpoint> filter(List<Endpoint> endpoints) {
        return endpoints;
    }

    @Override
    public List<Endpoint> order(List<Endpoint> endpoints) {
        return endpoints;
    }

    @Override
    public String getName() {
        return "all";
    }

}
