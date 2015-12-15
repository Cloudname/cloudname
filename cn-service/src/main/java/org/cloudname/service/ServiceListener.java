package org.cloudname.service;

/**
 * Listener interface for services.
 *
 * @author stalehd@gmail.com
 */
public interface ServiceListener {
    /**
     * Service is created. Note that this method is called once for every service that already
     * exists when the listener is attached.
     *
     * @param coordinate Coordinate of instance
     * @param serviceData The instance's data, ie its endpoints
     */
    void onServiceCreated(final InstanceCoordinate coordinate, final ServiceData serviceData);

    /**
     * Service's data have changed.
     * @param coordinate Coordinate of instance
     * @param data The instance's data
     */
    void onServiceDataChanged(final InstanceCoordinate coordinate, final ServiceData data);

    /**
     * Instance is removed. This means that the service has either closed its connection to
     * the Cloudname backend or it has become unavailable for some other reason (f.e. caused
     * by a network partition)
     *
     * @param coordinate The instance's coordinate
     */
    void onServiceRemoved(final InstanceCoordinate coordinate);
}
