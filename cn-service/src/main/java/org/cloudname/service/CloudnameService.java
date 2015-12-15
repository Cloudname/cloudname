package org.cloudname.service;

import org.cloudname.core.CloudnameBackend;
import org.cloudname.core.CloudnamePath;
import org.cloudname.core.LeaseHandle;
import org.cloudname.core.LeaseListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service discovery implementation. Use registerService() and addServiceListener() to register
 * and locate services.
 *
 * @author stalehd@gmail.com
 */
public class CloudnameService implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(CloudnameService.class.getName());

    private final CloudnameBackend backend;
    private final List<ServiceHandle> handles = new ArrayList<>();
    private final List<LeaseListener> temporaryListeners = new ArrayList<>();
    private final List<LeaseListener> permanentListeners = new ArrayList<>();
    private final Set<ServiceCoordinate> permanentUpdatesInProgress = new CopyOnWriteArraySet<>();
    private final Object syncObject = new Object();

    /**
     * Create the service interface.
     *
     * @oaram backend  backend implementation to use
     * @throws IllegalArgumentException if parameter is invalid
     */
    public CloudnameService(final CloudnameBackend backend) {
        if (backend == null) {
            throw new IllegalArgumentException("Backend can not be null");
        }
        this.backend = backend;
    }

    /**
     * Register an instance with the given service coordinate. The service will get its own
     * instance coordinate under the given service coordinate.
     *
     * @param serviceCoordinate The service coordinate that the service (instance) will attach to
     * @param serviceData  Service data for the instance
     * @return ServiceHandle  a handle the client can use to manage the endpoints for the service.
     *     The most typical use case is to register all endpoints
     * @throws IllegalArgumentException if the parameters are invalid
     */
    public ServiceHandle registerService(
            final ServiceCoordinate serviceCoordinate, final ServiceData serviceData) {

        if (serviceCoordinate == null) {
            throw new IllegalArgumentException("Coordinate cannot be null");
        }
        if (serviceData == null) {
            throw new IllegalArgumentException("Service Data cannot be null");
        }
        final LeaseHandle leaseHandle = backend.createTemporaryLease(
                serviceCoordinate.toCloudnamePath(), serviceData.toJsonString());

        final ServiceHandle serviceHandle = new ServiceHandle(
                new InstanceCoordinate(leaseHandle.getLeasePath()), serviceData, leaseHandle);

        synchronized (syncObject) {
            handles.add(serviceHandle);
        }
        return serviceHandle;
    }

    /**
     * Add listener for service events. This only applies to ordinary services.
     *
     * @param coordinate  The coordinate to monitor.
     * @param listener  Listener getting notifications on changes.
     * @throws IllegalArgumentException if parameters are invalid
     */
    public void addServiceListener(
            final ServiceCoordinate coordinate, final ServiceListener listener) {
        if (coordinate == null) {
            throw new IllegalArgumentException("Coordinate can not be null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("Listener can not be null");
        }
        // Just create the corresponding listener on the backend and translate the parameters
        // from the listener.
        final LeaseListener leaseListener = new LeaseListener() {
            @Override
            public void leaseCreated(final CloudnamePath path, final String data) {
                final InstanceCoordinate instanceCoordinate = new InstanceCoordinate(path);
                final ServiceData serviceData = ServiceData.fromJsonString(data);
                listener.onServiceCreated(instanceCoordinate, serviceData);
            }

            @Override
            public void leaseRemoved(final CloudnamePath path) {
                final InstanceCoordinate instanceCoordinate = new InstanceCoordinate(path);
                listener.onServiceRemoved(instanceCoordinate);
            }

            @Override
            public void dataChanged(final CloudnamePath path, final String data) {
                final InstanceCoordinate instanceCoordinate = new InstanceCoordinate(path);
                final ServiceData serviceData = ServiceData.fromJsonString(data);
                listener.onServiceDataChanged(instanceCoordinate, serviceData);
            }
        };
        synchronized (syncObject) {
            temporaryListeners.add(leaseListener);
        }
        backend.addTemporaryLeaseListener(coordinate.toCloudnamePath(), leaseListener);
    }

    /**
     * Create a permanent service. The service registration will be kept when the client exits. The
     * service will have a single endpoint.
     */
    public boolean createPermanentService(
            final ServiceCoordinate coordinate, final Endpoint endpoint) {
        if (coordinate == null) {
            throw new IllegalArgumentException("Service coordinate can't be null");
        }
        if (endpoint == null) {
            throw new IllegalArgumentException("Endpoint can't be null");
        }

        return backend.createPermanantLease(coordinate.toCloudnamePath(), endpoint.toJsonString());
    }

    /**
     * Update permanent service coordinate. Note that this is a non-atomic operation with multiple
     * trips to the backend system. The update is done in two operations; one delete and one
     * create. If the delete operation fail and the create operation succeeds it might end up
     * removing the permanent service coordinate. Clients will not be notified of the removal.
     */
    public boolean updatePermanentService(
            final ServiceCoordinate coordinate, final Endpoint endpoint) {
        if (coordinate == null) {
            throw new IllegalArgumentException("Coordinate can't be null");
        }
        if (endpoint == null) {
            throw new IllegalArgumentException("Endpoint can't be null");
        }

        if (permanentUpdatesInProgress.contains(coordinate)) {
            LOG.log(Level.WARNING, "Attempt to update a permanent service which is already"
                    + " updating. (coordinate: " + coordinate + ", endpoint: " + endpoint);
            return false;
        }
        // Check if the endpoint name still matches.
        final String data = backend.readPermanentLeaseData(coordinate.toCloudnamePath());
        if (data == null) {
            return false;
        }
        final Endpoint oldEndpoint = Endpoint.fromJson(data);
        if (!oldEndpoint.getName().equals(endpoint.getName())) {
            LOG.log(Level.INFO, "Rejecting attempt to update permanent service with a new endpoint"
                    + " that has a different name. Old name: " + oldEndpoint + " new: " + endpoint);
            return false;
        }
        permanentUpdatesInProgress.add(coordinate);
        try {
            return backend.writePermanentLeaseData(
                    coordinate.toCloudnamePath(), endpoint.toJsonString());
        } catch (final RuntimeException ex) {
            LOG.log(Level.WARNING, "Got exception updating permanent lease. The system might be in"
                    + " an indeterminate state", ex);
            return false;
        } finally {
            permanentUpdatesInProgress.remove(coordinate);
        }
    }

    /**
     * Remove a perviously registered permanent service. Needless to say: Use with caution.
     */
    public boolean removePermanentService(final ServiceCoordinate coordinate) {
        if (coordinate == null) {
            throw new IllegalArgumentException("Coordinate can not be null");
        }
        return backend.removePermanentLease(coordinate.toCloudnamePath());
    }

    /**
     * Listen for changes in permanent services. The changes are usually of the earth-shattering
     * variety so as a client you'd be interested in knowing about these as soon as possible.
     */
    public void addPermanentServiceListener(
            final ServiceCoordinate coordinate, final PermanentServiceListener listener) {
        if (coordinate == null) {
            throw new IllegalArgumentException("Coordinate can not be null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("Listener can not be null");
        }
        final LeaseListener leaseListener = new LeaseListener() {
            @Override
            public void leaseCreated(final CloudnamePath path, final String data) {
                listener.onServiceCreated(Endpoint.fromJson(data));
            }

            @Override
            public void leaseRemoved(final CloudnamePath path) {
                listener.onServiceRemoved();
            }

            @Override
            public void dataChanged(final CloudnamePath path, final String data) {
                listener.onServiceChanged(Endpoint.fromJson(data));
            }
        };
        synchronized (syncObject) {
            permanentListeners.add(leaseListener);
        }
        backend.addPermanentLeaseListener(coordinate.toCloudnamePath(), leaseListener);
    }

    @Override
    public void close() {
        synchronized (syncObject) {
            for (final ServiceHandle handle : handles) {
                handle.close();
            }
            for (final LeaseListener listener : temporaryListeners) {
                backend.removeTemporaryLeaseListener(listener);
            }
            for (final LeaseListener listener : permanentListeners) {
                backend.removePermanentLeaseListener(listener);
            }
        }
    }
}
