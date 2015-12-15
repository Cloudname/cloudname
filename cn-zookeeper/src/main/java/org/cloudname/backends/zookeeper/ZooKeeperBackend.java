package org.cloudname.backends.zookeeper;

import com.google.common.base.Charsets;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.cloudname.core.CloudnameBackend;
import org.cloudname.core.CloudnamePath;
import org.cloudname.core.LeaseHandle;
import org.cloudname.core.LeaseListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A ZooKeeper backend for Cloudname. Leases are represented as nodes; client leases are ephemeral
 * nodes inside container nodes and permanent leases are container nodes.
 *
 * @author stalehd@gmail.com
 */
public class ZooKeeperBackend implements CloudnameBackend {
    private static final Logger LOG = Logger.getLogger(ZooKeeperBackend.class.getName());
    private static final String TEMPORARY_ROOT = "/cn/temporary/";
    private static final String PERMANENT_ROOT = "/cn/permanent/";
    private static final int CONNECTION_TIMEOUT_SECONDS = 30;

    // PRNG for instance names. These will be "random enough" for instance identifiers
    private final Random random = new Random();
    private final CuratorFramework curator;
    private final Map<LeaseListener, NodeCollectionWatcher> clientListeners = new HashMap<>();
    private final Map<LeaseListener, NodeCollectionWatcher> permanentListeners = new HashMap<>();
    private final Object syncObject = new Object();

    /**
     * @param connectionString ZooKeeper connection string
     * @throws IllegalStateException if the cluster isn't available.
     */
    public ZooKeeperBackend(final String connectionString) {
        final RetryPolicy retryPolicy = new ExponentialBackoffRetry(200, 10);
        curator = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        curator.start();

        try {
            curator.blockUntilConnected(CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            LOG.info("Connected to zk cluster @ " + connectionString);
        } catch (final InterruptedException ie) {
            throw new IllegalStateException("Could not connect to ZooKeeper", ie);
        }
    }

    @Override
    public LeaseHandle createTemporaryLease(final CloudnamePath path, final String data) {
        boolean created = false;
        CloudnamePath tempInstancePath = null;
        String tempZkPath = null;
        while (!created) {
            final long instanceId = random.nextLong();
            tempInstancePath = new CloudnamePath(path, Long.toHexString(instanceId));
            tempZkPath = TEMPORARY_ROOT + tempInstancePath.join('/');
            try {

                curator.create()
                        .creatingParentContainersIfNeeded()
                        .withMode(CreateMode.EPHEMERAL)
                        .forPath(tempZkPath, data.getBytes(Charsets.UTF_8));
                created = true;
            } catch (final Exception ex) {
                LOG.log(Level.WARNING, "Could not create client node at " + tempInstancePath, ex);
            }
        }
        final CloudnamePath instancePath = tempInstancePath;
        final String zkInstancePath = tempZkPath;
        return new LeaseHandle() {
            private AtomicBoolean closed = new AtomicBoolean(false);

            @Override
            public boolean writeLeaseData(final String data) {
                if (closed.get()) {
                    LOG.info("Attempt to write data to closed leased handle " + data);
                    return false;
                }
                return writeTemporaryLeaseData(instancePath, data);
            }

            @Override
            public CloudnamePath getLeasePath() {
                if (closed.get()) {
                    return null;
                }
                return instancePath;
            }

            @Override
            public void close() throws IOException {
                if (closed.get()) {
                    return;
                }
                try {
                    curator.delete().forPath(zkInstancePath);
                    closed.set(true);
                } catch (final Exception ex) {
                    throw new IOException(ex);
                }
            }
        };
    }

    @Override
    public boolean writeTemporaryLeaseData(final CloudnamePath path, final String data) {
        final String zkPath = TEMPORARY_ROOT + path.join('/');
        try {
            final Stat nodeStat = curator.checkExists().forPath(zkPath);
            if (nodeStat == null) {
                LOG.log(Level.WARNING, "Could not write client lease data for " + path
                        + " with data since the path does not exist. Data = " + data);
            }
            curator.setData().forPath(zkPath, data.getBytes(Charsets.UTF_8));
            return true;
        } catch (final Exception ex) {
            LOG.log(Level.WARNING, "Got exception writing lease data to " + path
                    + " with data " + data);
            return false;
        }
    }

    @Override
    public String readTemporaryLeaseData(final CloudnamePath path) {
        if (path == null) {
            return null;
        }
        final String zkPath = TEMPORARY_ROOT + path.join('/');
        try {
            curator.sync().forPath(zkPath);
            final byte[] bytes = curator.getData().forPath(zkPath);
            return new String(bytes, Charsets.UTF_8);
        } catch (final Exception ex) {
            LOG.log(Level.WARNING, "Got exception reading client lease data at " + path, ex);
        }
        return null;
    }

    private CloudnamePath toCloudnamePath(final String zkPath, final String pathPrefix) {
        final String clientPath = zkPath.substring(pathPrefix.length());
        final String[] elements = clientPath.split("/");
        return new CloudnamePath(elements);
    }

    @Override
    public void addTemporaryLeaseListener(
            final CloudnamePath pathToObserve, final LeaseListener listener) {
        // Ideally the PathChildrenCache class in Curator would be used here to keep track of the
        // changes but it is ever so slightly broken and misses most of the watches that ZooKeeper
        // triggers, ignores the mzxid on the nodes and generally makes a mess of things. Enter
        // custom code.
        final String zkPath = TEMPORARY_ROOT + pathToObserve.join('/');
        try {
            curator.createContainers(zkPath);
            final NodeCollectionWatcher watcher = new NodeCollectionWatcher(
                    curator.getZookeeperClient().getZooKeeper(),
                    zkPath,
                    new NodeWatcherListener() {

                        @Override
                        public void nodeCreated(final String path, final String data) {
                            listener.leaseCreated(toCloudnamePath(path, TEMPORARY_ROOT), data);
                        }

                        @Override
                        public void dataChanged(final String path, final String data) {
                            listener.dataChanged(toCloudnamePath(path, TEMPORARY_ROOT), data);
                        }

                        @Override
                        public void nodeRemoved(final String path) {
                            listener.leaseRemoved(toCloudnamePath(path, TEMPORARY_ROOT));
                        }
                    });

            synchronized (syncObject) {
                clientListeners.put(listener, watcher);
            }
        } catch (final Exception exception) {
            LOG.log(Level.WARNING, "Got exception when creating node watcher", exception);
        }
    }

    @Override
    public void removeTemporaryLeaseListener(final LeaseListener listener) {
        synchronized (syncObject) {
            final NodeCollectionWatcher watcher = clientListeners.get(listener);
            if (watcher != null) {
                clientListeners.remove(listener);
                watcher.shutdown();
            }
        }
    }

    @Override
    public boolean createPermanantLease(final CloudnamePath path, final String data) {
        final String zkPath = PERMANENT_ROOT + path.join('/');
        try {
            curator.sync().forPath(zkPath);
            final Stat nodeStat = curator.checkExists().forPath(zkPath);
            if (nodeStat == null) {
                curator.create()
                        .creatingParentContainersIfNeeded()
                        .forPath(zkPath, data.getBytes(Charsets.UTF_8));
                return true;
            }
            LOG.log(Level.INFO, "Attempt to create permanent node at " + path
                    + " with data " + data + " but it already exists");
        } catch (final Exception ex) {
            LOG.log(Level.WARNING, "Got exception creating parent container for permanent lease"
                    + " for lease " + path + " with data " + data, ex);
        }
        return false;
    }

    @Override
    public boolean removePermanentLease(final CloudnamePath path) {
        final String zkPath = PERMANENT_ROOT + path.join('/');
        try {
            final Stat nodeStat = curator.checkExists().forPath(zkPath);
            if (nodeStat != null) {
                curator.delete()
                        .withVersion(nodeStat.getVersion())
                        .forPath(zkPath);
                return true;
            }
            return false;
        } catch (final Exception ex) {
            LOG.log(Level.WARNING, "Got error removing permanent lease for lease " + path, ex);
            return false;
        }
    }

    @Override
    public boolean writePermanentLeaseData(final CloudnamePath path, final String data) {
        final String zkPath = PERMANENT_ROOT + path.join('/');
        try {
            curator.sync().forPath(zkPath);
            final Stat nodeStat = curator.checkExists().forPath(zkPath);
            if (nodeStat == null) {
                LOG.log(Level.WARNING, "Can't write permanent lease data for lease " + path
                        + " with data " + data + " since the lease doesn't exist");
                return false;
            }
            curator.setData()
                    .withVersion(nodeStat.getVersion())
                    .forPath(zkPath, data.getBytes(Charsets.UTF_8));
        } catch (final Exception ex) {
            LOG.log(Level.WARNING, "Got exception writing permanent lease data for " + path
                    + " with data " + data, ex);
            return false;
        }
        return true;
    }

    @Override
    public String readPermanentLeaseData(final CloudnamePath path) {
        final String zkPath = PERMANENT_ROOT + path.join('/');
        try {
            curator.sync().forPath(zkPath);
            final byte[] bytes = curator.getData().forPath(zkPath);
            return new String(bytes, Charsets.UTF_8);
        } catch (final Exception ex) {
            if (ex instanceof KeeperException.NoNodeException) {
                // OK - nothing to worry about
                return null;
            }
            LOG.log(Level.WARNING, "Got exception reading permanent lease data for " + path, ex);
            return null;
        }
    }

    @Override
    public void addPermanentLeaseListener(
            final CloudnamePath pathToObserve, final LeaseListener listener) {
        try {

            final String parentPath = PERMANENT_ROOT + pathToObserve.getParent().join('/');
            final String fullPath = PERMANENT_ROOT + pathToObserve.join('/');
            curator.createContainers(parentPath);
            final NodeCollectionWatcher watcher = new NodeCollectionWatcher(
                    curator.getZookeeperClient().getZooKeeper(),
                    parentPath,
                    new NodeWatcherListener() {

                        @Override
                        public void nodeCreated(final String path, final String data) {
                            if (path.equals(fullPath)) {
                                listener.leaseCreated(toCloudnamePath(path, PERMANENT_ROOT), data);
                            }
                        }

                        @Override
                        public void dataChanged(final String path, final String data) {
                            if (path.equals(fullPath)) {
                                listener.dataChanged(toCloudnamePath(path, PERMANENT_ROOT), data);
                            }
                        }

                        @Override
                        public void nodeRemoved(final String path) {
                            if (path.equals(fullPath)) {
                                listener.leaseRemoved(toCloudnamePath(path, PERMANENT_ROOT));
                            }
                        }
                    });

            synchronized (syncObject) {
                permanentListeners.put(listener, watcher);
            }
        } catch (final Exception exception) {
            LOG.log(Level.WARNING, "Got exception when creating node watcher", exception);
        }
    }

    @Override
    public void removePermanentLeaseListener(final LeaseListener listener) {
        synchronized (syncObject) {
            final NodeCollectionWatcher watcher = permanentListeners.get(listener);
            if (watcher != null) {
                permanentListeners.remove(listener);
                watcher.shutdown();
            }
        }
    }

    @Override
    public void close() {
        synchronized (syncObject) {
            for (final NodeCollectionWatcher watcher : clientListeners.values()) {
                watcher.shutdown();
            }
            clientListeners.clear();
            for (final NodeCollectionWatcher watcher : permanentListeners.values()) {
                watcher.shutdown();
            }
            permanentListeners.clear();
        }
    }
}
