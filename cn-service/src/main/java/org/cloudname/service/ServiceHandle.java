package org.cloudname.service;

import org.cloudname.core.LeaseHandle;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A handle to a service registration. The handle is used to modify the registered endpoints. The
 * state is kept in the ServiceData instance held by the handle. Note that endpoints in the
 * ServiceData instance isn't registered automatically when the handle is created.
 *
 * @author stalehd@gmail.com
 */
public class ServiceHandle implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(ServiceHandle.class.getName());
    private final LeaseHandle leaseHandle;
    private final InstanceCoordinate instanceCoordinate;
    private final ServiceData serviceData;

    /**
     * Construct ServiceHandle from @link{InstanceCoordinate}, @link{ServiceData} and
     * @link{LeaseHandle} instances.
     *
     * @throws IllegalArgumentException if parameters are invalid
     */
    public ServiceHandle(
            final InstanceCoordinate instanceCoordinate,
            final ServiceData serviceData,
            final LeaseHandle leaseHandle) {
        if (instanceCoordinate == null) {
            throw new IllegalArgumentException("Instance coordinate cannot be null");
        }
        if (serviceData == null) {
            throw new IllegalArgumentException("Service data must be set");
        }
        if (leaseHandle == null) {
            throw new IllegalArgumentException("Lease handle cannot be null");
        }
        this.leaseHandle = leaseHandle;
        this.instanceCoordinate = instanceCoordinate;
        this.serviceData = serviceData;
    }

    /**
     * Register endpoint. Will update the backend's registration.
     *
     * @return true if successful
     */
    public boolean registerEndpoint(final Endpoint endpoint) {
        if (!serviceData.addEndpoint(endpoint)) {
            return false;
        }
        return this.leaseHandle.writeData(serviceData.toJsonString());
    }

    /**
     * Remove endpoint from service registration.
     *
     * @return true if successful
     */
    public boolean removeEndpoint(final Endpoint endpoint) {
        if (!serviceData.removeEndpoint(endpoint)) {
            return false;
        }
        return this.leaseHandle.writeData(serviceData.toJsonString());
    }

    @Override
    public void close() {
        try {
            leaseHandle.close();
        } catch (final Exception ex) {
            LOG.log(Level.WARNING, "Got exception closing lease for instance "
                    + instanceCoordinate.toCanonicalString(), ex);
        }
    }

    /**
     * Get the coordinate this @link{ServiceHandle} instance represents.
     */
    public InstanceCoordinate getCoordinate() {
        return instanceCoordinate;
    }
}
