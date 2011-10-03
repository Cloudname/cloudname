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
     * @param coordinate the coordinate we wish to create
     */
    public void createCoordinate(Coordinate coordinate);

    /**
     * Get the ServiceStatus for a given Coordinate.
     *
     * @param coordinate the coordinate we want to get the status of
     * @return a ServiceStatus instance.
     */
    public ServiceStatus getStatus(Coordinate coordinate);
}