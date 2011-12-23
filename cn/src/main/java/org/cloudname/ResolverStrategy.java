package org.cloudname;


import java.util.List;

public interface ResolverStrategy {
    /**
     * Given a list of endpoints, return only those endpoints that are desired for this strategy.
     */
    public List<Endpoint> filter(List<Endpoint> endpoints);

    /**
     * Returns the endpoints ordered according to strategy specific scheme.
     */
    public List<Endpoint> order(List<Endpoint> endpoints);

    /**
     * Returns the name of this strategy. This is the same name that is used in the resolver
     * (e.g. "all", "any" etc).
     * @return name of strategy.
     */
    public String getName();
}
