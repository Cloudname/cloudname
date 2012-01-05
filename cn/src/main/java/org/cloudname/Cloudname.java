package org.cloudname;

/**
 * The main interface for interacting with Cloudname.
 *
 * @author borud
 */
public interface Cloudname {
    /**
     * Claim a coordinateFlag returning a {@link ServiceHandle} through
     * which the service can interact with the system.  If the
     * coordinateFlag has already been claimed by a different running
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
     * Create a coordinateFlag in the persistent service store.  Must
     * throw an exception if the coordinateFlag has already been defined.
     *
     * @param coordinate the coordinateFlag we wish to create
     */
    public void createCoordinate(Coordinate coordinate);

    /**
     * Deletes a coordinateFlag in the persistent service store. It will fail if the coordinateFlag is claimed.
     * @param coordinate the coordinateFlag we wish to destroy.
     */
    public void destroyCoordinate(Coordinate coordinate);
    
    /**
     * Get the ServiceStatus for a given Coordinate.
     *
     * @param coordinate the coordinateFlag we want to get the status of
     * @return a ServiceStatus instance.
     */
    public ServiceStatus getStatus(Coordinate coordinate);
}