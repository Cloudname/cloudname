package org.cloudname;

/**
 * The main interface for interacting with Cloudname.
 *
 * @author borud
 */
public interface Cloudname {
    /**
     * Claim a coordinate returning a {@link ServiceHandle} through
     * which the service can interact with the system.  If the
     * coordinate has already been claimed by a different running
     * instance of the service, an exception will be thrown.
     *
     * @param coordinate of the service we wish to claim
     * @throws CoordinateMissingException if coordinate is missing.
     * @throws CoordinateAlreadyClaimedException if coordinate is already claimed.
     * @throws CloudnameException if problems talking with storage.
     */
    public ServiceHandle claim(Coordinate coordinate)
            throws CloudnameException, CoordinateMissingException, CoordinateAlreadyClaimedException;

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
}