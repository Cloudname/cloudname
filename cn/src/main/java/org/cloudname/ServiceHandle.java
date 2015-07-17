package org.cloudname;

import java.util.List;
import org.cloudname.CoordinateListener.Event;

/**
 * The service handle -- the interface through which services
 * communicate their state to the outside world and where services can
 * register listeners to handle configuration updates.
 *
 * @author borud
 */
public interface ServiceHandle {

    /**
     * This is a convenient function for waiting for the connection to storage
     * to be ok. It is the same as registering a CoordinateListener and waiting
     * for a given event.
     */
    public boolean waitForCoordinateOkSeconds(int seconds) throws InterruptedException;

    /**
     * Convenience method waiting for a coordinate ok event.
     */
    public boolean waitForCoordinateEventSeconds(final int seconds, final Event event)
            throws InterruptedException;

    /**
     * Set the status of this service.
     *
     * @param status the new status.
     * @throws CoordinateMissingException if coordinate does not exist.
     * @throws CloudnameException if coordinate is not claimed, connection to storage is down, or problems
     * with ZooKeeper.
     */
    public void setStatus(ServiceStatus status) throws CoordinateMissingException, CloudnameException;

    /**
     * Publish a named endpoint.  It is legal to push an endpoint with updated data.
     *
     * @param endpoint the endpoint data.
     * @throws CoordinateMissingException if coordinate does not exist.
     * @throws CloudnameException if coordinate is not claimed, connection to storage is down, or problems
     * with ZooKeeper.
     */
    public void putEndpoint(Endpoint endpoint) throws CoordinateMissingException, CloudnameException;

    /**
     * Same as putEndpoints, but takes a list.
     *
     * @param endpoints the endpoints data.
     * @throws CloudnameException if coordinate is not claimed, connection to storage is down, or problems
     * with ZooKeeper.
     * @throws CoordinateMissingException if coordinate does not exist.
     */
    public void putEndpoints(List<Endpoint> endpoints) throws CoordinateMissingException, CloudnameException;

    /**
     * Remove a published endpoint.
     *
     * @param name the name of the endpoint we wish to remove.
     * @throws CoordinateMissingException if coordinate does not exist.
     * @throws CloudnameException if coordinate is not claimed, connection to storage is down, or problems
     * with ZooKeeper.
     */
    public void removeEndpoint(String name) throws CoordinateMissingException, CloudnameException;

    /**
     * Same as removeEndpoint() but takes a list of names.
     *
     * @throws CloudnameException if coordinate is not claimed, connection to storage is down, or problems
     * with ZooKeeper.
     * @throws CoordinateMissingException if coordinate does not exist.
     * @throws CoordinateMissingException if coordinate does not exist.
     */
    public void removeEndpoints(List<String> names) throws CoordinateMissingException, CloudnameException;


    /**
     * Register a ConfigListener which will be called whenever there
     * is a configuration change.
     *
     * There may have been configuration pushed to the backing storage
     * already by the time a ConfigListener is registered.  In that
     * case the ConfigListener will see these configuration items as
     * being created.
     */
    // TODO(dybdahl): This logic lacks tests. Before used in any production code, tests have to be added.
    public void registerConfigListener(ConfigListener listener);

    /**
     * After registering a new listener, a new event is triggered which include current state, even without change
     * of state.
     * Don't call the cloudname library, do any heavy lifting, or do any IO operation from this callback thread.
     * That might deadlock as there is no guarantee what kind of thread that runs the callback.
     *
     * @throws CloudnameException if problems talking with storage.
     */
    public void registerCoordinateListener(CoordinateListener listener)
            throws CloudnameException;

    /**
     * Close the service handle and free up the coordinate so it can
     * be claimed by others.  After close() has been called all
     * operations on this instance of the service handle will result
     * in an exception being thrown. All endpoints are deleted.
     * @throws CloudnameException if problems removing the claim.
     */
    public void close()
            throws CloudnameException;

    /**
     * Get a CloudnameLock object.
     * @param scope The scope of the coordinate you want to place the lock on.
     * @param lockName The String identifying the lock across the level selected.
     * @return CloudnameLock
     */
    // TODO (acidmoose): Revisit api for creating CloudnameLock object.
    // Should perhaps be a default Scope set to Scope.SERVICE.
    public CloudnameLock getCloudnameLock(CloudnameLock.Scope scope, String lockName);
}

