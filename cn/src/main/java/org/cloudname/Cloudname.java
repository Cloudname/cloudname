package org.cloudname;

import org.cloudname.zk.ZkCloudnameLock;

/**
 * The main interface for interacting with Cloudname.
 *
 * @author borud
 * @author dybdahl
 */
public interface Cloudname {
    /**
     * Claim a coordinate returning a {@link ServiceHandle} through
     * which the service can interact with the system. This is an asynchronous operation, to check result
     * use the returned Servicehandle. E.g. for waiting up to ten seconds for a claim to happen:
     *
     * Cloudname cn = ...
     * Coordinate coordinate = ...
     * ServiceHandle serviceHandle = cn.claim(coordinate);
     * final CountDownLatch latch = new CountDownLatch(1);
     * boolean claimSuccess = serviceHandle.waitForCoordinateOkSeconds(10);
     *
     * @param coordinate of the service we wish to claim.
     * @return a ServiceHandle that can wait for the claim to be successful and listen to the state of the claim.
     */
    public ServiceHandle claim(Coordinate coordinate);

    /**
     * Get a resolver instance.
     */
    public Resolver getResolver();

    /**
     * Create a coordinate in the persistent service store.  Must
     * throw an exception if the coordinate has already been defined.
     *
     *
     * @param coordinate the coordinate we wish to create
     * @throws CoordinateExistsException if coordinate already exists.
     * @throws CloudnameException if problems with talking with storage.
     */
    public void createCoordinate(Coordinate coordinate)
            throws CloudnameException, CoordinateExistsException;

    /**
     * Deletes a coordinate in the persistent service store. It will throw an exception if the coordinate is claimed.
     * @param coordinate the coordinate we wish to destroy.
     * @throws CoordinateMissingException if coordinate does not exist.
     * @throws CloudnameException if problems talking with storage.
     * @throws CoordinateDeletionException if problems occurred during deletion.
     */
    public void destroyCoordinate(Coordinate coordinate)
            throws CoordinateDeletionException, CoordinateMissingException, CloudnameException;
    
    /**
     * Get the ServiceStatus for a given Coordinate.
     *
     * @param coordinate the coordinate we want to get the status of
     * @return a ServiceStatus instance.
     * @throws CloudnameException if problems with talking with storage.
     */
    public ServiceStatus getStatus(Coordinate coordinate)
            throws CloudnameException;

    /**
     * Updates the config for a coordinate. If the oldConfig is set (not null) it will require that the old config
     * matches otherwise it will throw an exception
     * @throws CoordinateMissingException if coordinate does not exist.
     * @throws CloudnameException if problems including oldConfig does not match old config.
     */
    public void setConfig(final Coordinate coordinate, final String newConfig, final String oldConfig)
     throws CoordinateMissingException, CloudnameException;

    /**
     * Get config for a coordinate.
     * @return the new config.
     * @throws CoordinateMissingException if coordinate does not exist.
     * @throws CloudnameException
     */
    public String getConfig(final Coordinate coordinate)
            throws CoordinateMissingException, CloudnameException;
}
