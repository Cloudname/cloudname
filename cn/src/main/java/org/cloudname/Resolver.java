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
     * Resolve an expression to a list of endpoints.  The order of the
     * endpoints may be subject to ranking criteria.
     *
     * @param expression The expression to resolve, e.g. for ZooKeeper implementation there are various formats like
     *                   endpoint.instance.service.user.cell (see ZkResolver for details).
     * @throws CloudnameException if problems talking with storage.
     */
    public List<Endpoint> resolve(String expression) throws CloudnameException;


    /**
     * Implement this interface to get dynamic information about what endpoints that are available.
     */
    public interface ResolverListener {
        public enum Event {
            /**
             * New endpoint was added.
             */
            NEW_ENDPOINT,
            /**
             * Endpoint removed. This include when the coordinate goes to draining.
             */
            REMOVED_ENDPOINT,
            /**
             * Lost connection to storage. The list of endpoints will get stale. The system will reconnect
             * automatically.
             */
            LOST_CONNECTION,
            /**
             * Connection to storage is good, list of endpoints will be updated.
             */
            CONNECTION_OK
        }

        /**
         * An Event happened related to the expression, see enum Event above.
         * @param endpointId this is a unique id for the endpoint and coordinate.
         * @param endpoint is only populated for the Event NEW_ENDPOINT and REMOVED_ENDPOINT.
         */
        void endpointEvent(Event event, String endpointId, final Endpoint endpoint);
    }

    /**
     * Registers a ResolverListener to get dynamic information about an expression.
     * You will only get updates as long as you keep a reference to Resolver. If you don't have a reference
     * it is up to the garbage collector to decide how long you will receive callbacks.
     * @param expression The expression to resolve, e.g. for ZooKeeper implementation there are various formats like
     *                   endpoint.instance.service.user.cell (see ZkResolver for details).
     */
    public void addResolverListener(String expression, ResolverListener listener);
}
