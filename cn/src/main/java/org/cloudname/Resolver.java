package org.cloudname;

import java.util.List;
import java.util.Set;

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
     * If you want to register more than 1000 listeners in the same resolver, you might consider overriding
     * equals() and hashCode(), but the default implementation should work in normal cases.
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
         * @param endpoint is only populated for the Event NEW_ENDPOINT and REMOVED_ENDPOINT.
         */
        void endpointEvent(Event event, final Endpoint endpoint);
    }
    
    /**
     * Registers a ResolverListener to get dynamic information about an expression. The expression is set in the
     * ResolverListener. You will only get updates as long as you keep a reference to Resolver.
     * If you don't have a reference, it is up to the garbage collector to decide how long you will receive callbacks.
     * One listener can only be registered once.
     *
     * @param expression The expression to resolve, e.g. for ZooKeeper implementation there are various formats like
     * endpoint.instance.service.user.cell (see ZkResolver for details). This should be static data, i.e.
     * the function might be called only once.
     */
    public void addResolverListener(String expression, ResolverListener listener) throws CloudnameException;

    /**
     * Calling this function unregisters the listener, i.e. stopping future callbacks.
     * The listener must be registered. For identification of listener, see comment on ResolverListener.
     * The default is to use object id.
     */
    public void removeResolverListener(ResolverListener listener);

    /**
     * This class is used as a parameter to {@link #getEndpoints(CoordinateDataFilter)}. Override methods to filter
     * the endpoints to be
     * returned.
     */
    public class CoordinateDataFilter {
        /**
         * Override these methods to filter on cell, user, service, endpointName, and/or service state.
         */

        public boolean includeCell(final String cell) {
            return true;
        }
        public boolean includeUser(final String user) {
            return true;
        }
        public boolean includeService(final String service) {
            return true;
        }
        public boolean includeEndpointname(final String endpointName) {
            return true;
        }
        public boolean includeServiceState(final ServiceState state) {
            return true;
        }
    }

    /**
     * This method reads out all the nodes from the storage. IT CAN BE VERY EXPENSIVE AND SHOULD BE USED ONLY
     * WHEN NO OTHER METHODS ARE FEASIBLE. Do not call it frequently!
     * @param filter class for filtering out endpoints
     * @return list of endpoints.
     */
    public Set<Endpoint> getEndpoints(CoordinateDataFilter filter) throws CloudnameException, InterruptedException;
}
