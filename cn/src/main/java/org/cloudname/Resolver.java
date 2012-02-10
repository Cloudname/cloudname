package org.cloudname;


import java.util.List;

/**
 * This interface defines how we resolve endpoints in Cloudname. The client has to keep a reference to this Resolver
 * object otherwise it will stop resolving.
 *
 * @author borud
 */
public interface Resolver {

    /**
     * Resolve an address to a list of endpoints.  The order of the
     * endpoints may be subject to ranking criteria.
     *
     * @param address the address of the endpoint(s).
     * @throws CloudnameException if problems talking with storage.
     */
    public List<Endpoint> resolve(String address) throws CloudnameException;


    public interface ResolverFuture {
        void endpointModified(final Endpoint endpoint);
        void endpointDeleted(final Endpoint endpoint);
    }

    public void addResolverListener(String address, ResolverFuture future);

    public void shutdown();
}