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
     * @param instanceCoordinate The instance coordinate this handle belongs to
     * @param serviceData The service data object
     * @param leaseHandle The Cloudname handle for the lease
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
     * @param endpoint The endpoint to register
     * @return true if endpoint is registered
     */
    public boolean registerEndpoint(final Endpoint endpoint) {
        if (!serviceData.addEndpoint(endpoint)) {
            return false;
        }
        return this.leaseHandle.writeLeaseData(serviceData.toJsonString());
    }

    /**
     * @param endpoint The endpoint to remove
     * @return true if endpoint is removed
     */
    public boolean removeEndpoint(final Endpoint endpoint) {
        if (!serviceData.removeEndpoint(endpoint)) {
            return false;
        }
        return this.leaseHandle.writeLeaseData(serviceData.toJsonString());
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

    public InstanceCoordinate getCoordinate() {
        return instanceCoordinate;
    }
}
