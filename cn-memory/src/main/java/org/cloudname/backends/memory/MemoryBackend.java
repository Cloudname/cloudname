package org.cloudname.backends.memory;

import org.cloudname.core.CloudnameBackend;
import org.cloudname.core.CloudnamePath;
import org.cloudname.core.LeaseHandle;
import org.cloudname.core.LeaseListener;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Memory backend. This is the canonical implementation. The synchronization is probably not
 * optimal but for testing this is OK. It defines the correct behaviour for backends, including
 * calling listeners, return values and uniqueness. The actual timing of the various backends
 * will of course vary.
 *
 * @author stalehd@gmail.com
 */
public class MemoryBackend implements CloudnameBackend {
    private enum LeaseEvent {
        CREATED,
        REMOVED,
        DATA
    }

    private final Map<CloudnamePath,String> temporaryLeases = new HashMap<>();
    private final Map<CloudnamePath,String> permanentLeases = new HashMap<>();
    private final Map<CloudnamePath, Set<LeaseListener>> observedTemporaryPaths = new HashMap<>();
    private final Map<CloudnamePath, Set<LeaseListener>> observedPermanentPaths = new HashMap<>();
    private final Object syncObject = new Object();

    /* package-private */ void removeTemporaryLease(final CloudnamePath leasePath) {
        synchronized (syncObject) {
            if (temporaryLeases.containsKey(leasePath)) {
                temporaryLeases.remove(leasePath);
                notifyTemporaryObservers(leasePath, LeaseEvent.REMOVED, null);
            }
        }
    }
    private final Random random = new Random();

    private String createRandomInstanceName() {
        return Long.toHexString(random.nextLong());
    }

    /**
     * @param path The path that has changed
     * @param event The event
     * @param data The data
     */
    private void notifyTemporaryObservers(
            final CloudnamePath path, final LeaseEvent event, final String data) {
        for (final CloudnamePath observedPath : observedTemporaryPaths.keySet()) {
            if (observedPath.isSubpathOf(path)) {
                for (final LeaseListener listener : observedTemporaryPaths.get(observedPath)) {
                    switch (event) {
                        case CREATED:
                            listener.leaseCreated(path, data);
                            break;
                        case REMOVED:
                            listener.leaseRemoved(path);
                            break;
                        case DATA:
                            listener.dataChanged(path, data);
                            break;
                        default:
                            throw new RuntimeException("Don't know how to handle " + event);
                    }
                }
            }
        }
    }

    /**
     * Notify observers of changes
     */
    private void notifyPermanentObservers(
            final CloudnamePath path, final LeaseEvent event, final String data) {
        for (final CloudnamePath observedPath : observedPermanentPaths.keySet()) {
            if (observedPath.isSubpathOf(path)) {
                for (final LeaseListener listener : observedPermanentPaths.get(observedPath)) {
                    switch (event) {
                        case CREATED:
                            listener.leaseCreated(path, data);
                            break;
                        case REMOVED:
                            listener.leaseRemoved(path);
                            break;
                        case DATA:
                            listener.dataChanged(path, data);
                            break;
                        default:
                            throw new RuntimeException("Don't know how to handle " + event);
                    }
                }
            }
        }
    }

    @Override
    public boolean createPermanantLease(final CloudnamePath path, final String data) {
        assert path != null : "Path to lease must be set!";
        assert data != null : "Lease data is required";
        synchronized (syncObject) {
            if (permanentLeases.containsKey(path)) {
                return false;
            }
            permanentLeases.put(path, data);
            notifyPermanentObservers(path, LeaseEvent.CREATED, data);
        }
        return true;
    }

    @Override
    public boolean removePermanentLease(final CloudnamePath path) {
        synchronized (syncObject) {
            if (!permanentLeases.containsKey(path)) {
                return false;
            }
            permanentLeases.remove(path);
            notifyPermanentObservers(path, LeaseEvent.REMOVED, null);
        }
        return true;
    }

    @Override
    public boolean writePermanentLeaseData(final CloudnamePath path, String data) {
        synchronized (syncObject) {
            if (!permanentLeases.containsKey(path)) {
                return false;
            }
            permanentLeases.put(path, data);
            notifyPermanentObservers(path, LeaseEvent.DATA, data);
        }
        return true;
    }

    @Override
    public String readPermanentLeaseData(final CloudnamePath path) {
        synchronized (syncObject) {
            if (!permanentLeases.containsKey(path)) {
                return null;
            }
            return permanentLeases.get(path);
        }
    }

    @Override
    public boolean writeTemporaryLeaseData(final CloudnamePath path, String data) {
        synchronized (syncObject) {
            if (!temporaryLeases.containsKey(path)) {
                return false;
            }
            temporaryLeases.put(path, data);
            notifyTemporaryObservers(path, LeaseEvent.DATA, data);
        }
        return true;
    }

    @Override
    public String readTemporaryLeaseData(final CloudnamePath path) {
        synchronized (syncObject) {
            if (!temporaryLeases.containsKey(path)) {
                return null;
            }
            return temporaryLeases.get(path);
        }
    }

    @Override
    public LeaseHandle createTemporaryLease(final CloudnamePath path, final String data) {
        synchronized (syncObject) {
            final String instanceName = createRandomInstanceName();
            CloudnamePath instancePath = new CloudnamePath(path, instanceName);
            while (temporaryLeases.containsKey(instancePath)) {
                instancePath = new CloudnamePath(path, instanceName);
            }
            temporaryLeases.put(instancePath, data);
            notifyTemporaryObservers(instancePath, LeaseEvent.CREATED, data);
            return new MemoryLeaseHandle(this, instancePath);
        }
    }

    /**
     * Generate created events for temporary leases for newly attached listeners.
     */
    private void regenerateEventsForTemporaryListener(
            final CloudnamePath path, final LeaseListener listener) {
       temporaryLeases.keySet().forEach((temporaryPath) -> {
           if (path.isSubpathOf(temporaryPath)) {
               listener.leaseCreated(temporaryPath, temporaryLeases.get(temporaryPath));
           }
       });
    }

    /**
     * Generate created events on permanent leases for newly attached listeners.
     */
    private void regenerateEventsForPermanentListener(
            final CloudnamePath path, final LeaseListener listener) {
        permanentLeases.keySet().forEach((permanentPath) -> {
            if (path.isSubpathOf(permanentPath)) {
                listener.leaseCreated(permanentPath, permanentLeases.get(permanentPath));
            }
        });
    }

    @Override
    public void addTemporaryLeaseListener(
            final CloudnamePath pathToObserve, final LeaseListener listener) {
        synchronized (syncObject) {
            Set<LeaseListener> listeners = observedTemporaryPaths.get(pathToObserve);
            if (listeners == null) {
                listeners = new HashSet<>();
            }
            listeners.add(listener);
            observedTemporaryPaths.put(pathToObserve, listeners);
            regenerateEventsForTemporaryListener(pathToObserve, listener);
        }
    }

    @Override
    public void removeTemporaryLeaseListener(final LeaseListener listener) {
        synchronized (syncObject) {
            for (final Set<LeaseListener> listeners : observedTemporaryPaths.values()) {
                if (listeners.contains(listener)) {
                    listeners.remove(listener);
                    return;
                }
            }
        }
    }

    @Override
    public void addPermanentLeaseListener(
            final CloudnamePath pathToObserve, final LeaseListener listener) {
        synchronized (syncObject) {
            Set<LeaseListener> listeners = observedPermanentPaths.get(pathToObserve);
            if (listeners == null) {
                listeners = new HashSet<>();
            }
            listeners.add(listener);
            observedPermanentPaths.put(pathToObserve, listeners);
            regenerateEventsForPermanentListener(pathToObserve, listener);
        }
    }

    @Override
    public void removePermanentLeaseListener(final LeaseListener listener) {
        synchronized (syncObject) {
            for (final Set<LeaseListener> listeners : observedPermanentPaths.values()) {
                if (listeners.contains(listener)) {
                    listeners.remove(listener);
                    return;
                }
            }
        }
    }

    @Override
    public void close() {
        synchronized (syncObject) {
            observedTemporaryPaths.clear();
            observedPermanentPaths.clear();
        }
    }
}
