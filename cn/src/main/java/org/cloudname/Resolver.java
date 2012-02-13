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


    /**
     * Implement this interface to get dynamic information about what endpoints that are available.
     */
    public interface ResolverFuture {
        /**
         * The endpoint has become available or something has changed.
         */
        void endpointModified(final Endpoint endpoint);

        /**
         * The endpoint should no longer be accessed.
         * @param endpoint
         */
        void endpointDeleted(final Endpoint endpoint);
    }

    /**
     * Registers a ResolverFuture to get dynamic information about an address.
     * You will only get updates as long as you keep a reference to Resolver. If you don't have a reference
     * it is up to the garbage collector to decide how long you will receive callbacks.
     */
    public void addResolverListener(String address, ResolverFuture future);

    /**
     * This stops the futures from being called. Not allowed to do operations on the object after this method has been
     * called.
     */
    public void shutdown();
}