package org.cloudname.backends.consul;

import org.cloudname.core.CloudnameBackend;
import org.cloudname.core.CloudnamePath;
import org.cloudname.core.LeaseHandle;
import org.cloudname.core.LeaseListener;
import org.cloudname.core.LeaseType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * This is a basic implementation of a CloudName backend. It uses the KV store for all data since
 * the service concept doesn't fit too well with the lease concept. The KV store in conjunction
 * with sessions fits nicely though. This is currently a proof-of-concept implementation tha haven't
 * been tested extensively.
 *
 * @author stalehd@gmail.com
 */
public class ConsulBackend implements CloudnameBackend {
    private static final Logger LOG = Logger.getLogger(ConsulBackend.class.getName());

    final Consul consul;

    private static final int SESSION_TTL = 10;
    private static final int LOCK_DELAY = 0;

    private final Map<CloudnamePath, ConsulSession> sessions = new ConcurrentHashMap<>();
    private final Map<LeaseListener, ConsulWatch> watches = new ConcurrentHashMap<>();
    private static final char SEPARATOR = '/';
    private static final String CN_PREFIX = "cn";

    /**
     * Convert a cloudname path to a session name.
     */
    private String pathToSession(final CloudnamePath path) {
        return CN_PREFIX + SEPARATOR + path.join(SEPARATOR);
    }

    /**
     * Convert a cloudname path to a KV key name.
     */
    private String pathToKv(final CloudnamePath path) {
        return CN_PREFIX + SEPARATOR + SEPARATOR + path.join(SEPARATOR);
    }

    /**
     * Convert ephemeral or permanent key name into a Cloudname path.
     */
    private CloudnamePath kvNameToCloudnamePath(final String name) {
        final String[] elements = name.split("" + SEPARATOR);
        // The first two elements are prefixes; skip those
        return new CloudnamePath(Arrays.copyOfRange(elements, 2, elements.length));
    }

    /**
     * Create new backend connected to the specified endpoint.
     *
     * @throws IllegalArgumentException  the endpoint doesn't exist
     */
    public ConsulBackend(final String consulEndpoint) {
        consul = new Consul(consulEndpoint);
        if (!consul.isValid()) {
            throw new IllegalArgumentException("Consul endpoint " + consulEndpoint
                    + " isn't a valid endpoint");
        }
    }

    // Use a regular random value. Since this is an instance identifier which is well known
    // there's no need for particular randomness.
    private final Random random = new Random();

    // Since Consul doesn't allow cas=0 and acquire=<session id> at the same time we'll have
    // to keep track of the instance IDs we've created
    private final Set<String> createdIds = new HashSet<>();
    private final Object syncObject = new Object();

    /**
     * Get an instance ID with a random name. Ensures that the random instance id isn't used
     * before by this instance. We'll just have to assume that the locks works without race
     * conditions across the cluster. They probably do.
     */
    private String getRandomInstanceId() {
        synchronized (syncObject) {
            String id = Long.toHexString(random.nextLong());
            while (createdIds.contains(id)) {
                id = Long.toHexString(random.nextLong());
            }
            createdIds.add(id);
            return id;
        }
    }

    private LeaseHandle createTemporary(final CloudnamePath path, final String data) {
        // Create session with TTL set to <something> and Behavior=delete. The session isn't
        // used to uniquely identify the client but to create ephemeral values in the KV store.
        final ConsulSession session
                = consul.createSession(pathToSession(path), SESSION_TTL, LOCK_DELAY);
        // Create value in KV and set the session owner. cas = 0 to ensure no duplicates. The KV
        // entry is the canonical lease
        boolean leaseAcquired = false;
        final AtomicReference<CloudnamePath> instancePath = new AtomicReference<>();
        while (!leaseAcquired) {
            instancePath.set(new CloudnamePath(path, getRandomInstanceId()));
            leaseAcquired = consul.writeSessionData(
                    pathToKv(instancePath.get()), data, session.getId());
        }

        sessions.put(instancePath.get(), session);
        // Optional: Create service and set the session (so that the service appears in DNS)
        //    health check for service is lookup in KV store. The service entry is FYI only

        return new LeaseHandle() {
            @Override
            public boolean writeData(final String data) {
                if (session.isClosed()) {
                    return false;
                }
                return consul.writeSessionData(
                        pathToKv(instancePath.get()), data, session.getId());
            }

            @Override
            public CloudnamePath getLeasePath() {
                if (session.isClosed()) {
                    return null;
                }
                return instancePath.get();
            }

            @Override
            public void close() throws Exception {
                // This will clear the KV entry
                session.close();
                sessions.remove(instancePath.get());
            }
        };
    }

    @Override
    public boolean writeLeaseData(final CloudnamePath path, final String data) {
        final ConsulSession session = sessions.get(path);
        if (session == null) {
            return false;
        }
        return consul.writeSessionData(pathToKv(path), data, session.getId());
    }

    @Override
    public String readLeaseData(final CloudnamePath path) {
        if (path == null) {
            return null;
        }
        return consul.readData(pathToKv(path));
    }

    @Override
    public LeaseHandle createLease(
            final LeaseType type, final CloudnamePath path, final String data) {
        switch (type) {
            case PERMANENT:
                if (consul.createPermanentData(pathToKv(path), data)) {
                    return new LeaseHandle() {
                        @Override
                        public boolean writeData(final String data) {
                            return writeLeaseData(path, data);
                        }

                        @Override
                        public CloudnamePath getLeasePath() {
                            return path;
                        }

                        @Override
                        public void close() throws Exception {
                            // nothing to do
                        }
                    };
                }
                return null;

            case TEMPORARY:
                return createTemporary(path, data);

            default:
                LOG.severe("Uknown lease type: " + type
                        + " - don't know how to create that kind of lease"
                        + " (path = " + path + ", data = " + data + ")");
                return null;
        }
    }

    @Override
    public boolean removeLease(final CloudnamePath path) {
        final String consulPath = pathToKv(path);
        if (consul.readData(consulPath) == null) {
            return false;
        }
        return consul.removePermanentData(consulPath);
    }

    @Override
    public void addLeaseListener(final CloudnamePath leaseToObserve, final LeaseListener listener) {
        final ConsulWatch watch = consul.createWatch(pathToKv(leaseToObserve));
        watches.put(listener, watch);
        watch.startWatching(new ConsulWatch.ConsulWatchListener() {
            @Override
            public void created(final String valueName, final String value) {
                final CloudnamePath path = kvNameToCloudnamePath(valueName);
                if (path.equals(leaseToObserve)) {
                    listener.leaseCreated(path, value);
                }
            }

            @Override
            public void changed(final String valueName, final String value) {
                final CloudnamePath path = kvNameToCloudnamePath(valueName);
                if (path.equals(leaseToObserve)) {
                    listener.dataChanged(path, value);
                }
            }

            @Override
            public void removed(final String valueName) {
                final CloudnamePath path = kvNameToCloudnamePath(valueName);
                if (path.equals(leaseToObserve)) {
                    listener.leaseRemoved(path);
                }
            }
        });
    }

    @Override
    public void addLeaseCollectionListener(
            final CloudnamePath pathToObserve, final LeaseListener listener) {
        final ConsulWatch watch = consul.createWatch(pathToKv(pathToObserve));
        watches.put(listener, watch);
        watch.startWatching(new ConsulWatch.ConsulWatchListener() {
            @Override
            public void created(final String valueName, final String value) {
                listener.leaseCreated(kvNameToCloudnamePath(valueName), value);
            }

            @Override
            public void changed(final String valueName, final String value) {
                listener.dataChanged(kvNameToCloudnamePath(valueName), value);
            }

            @Override
            public void removed(final String valueName) {
                listener.leaseRemoved(kvNameToCloudnamePath(valueName));
            }
        });
    }

    @Override
    public void removeLeaseListener(final LeaseListener listener) {
        final ConsulWatch watch = watches.get(listener);
        if (watch != null) {
            watch.stop();
        }
    }

    @Override
    public void close() {
        watches.forEach((listener, watch) -> watch.stop());
        sessions.forEach((listener, session) -> session.close());
    }
}
