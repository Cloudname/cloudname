package org.cloudname.service;

/**
 * Listener interface for permanent services.
 *
 * @author stalehd@gmail.com
 */
public interface PermanentServiceListener {
    /**
     * A service is created. This method will be called on start-up for all existing services.
     * @param endpoint The endpoint of the service
     */
    void onServiceCreated(final Endpoint endpoint);

    /**
     * Service endpoint has changed.
     * @param endpoint The new value of the service endpoint
     */
    void onServiceChanged(final Endpoint endpoint);

    /**
     * Service has been removed.
     */
    void onServiceRemoved();
}
