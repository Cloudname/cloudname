package org.cloudname.backends.zookeeper;

import com.google.common.base.Charsets;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.cloudname.core.CloudnameBackend;
import org.cloudname.core.CloudnamePath;
import org.cloudname.core.LeaseHandle;
import org.cloudname.core.LeaseListener;
import org.cloudname.core.LeaseType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
    private static final String ZK_ROOT = "/cn/";
    private static final int CONNECTION_TIMEOUT_SECONDS = 30;

    private final CuratorFramework curator;
    private final Map<LeaseListener, NodeCollectionWatcher> collectionListeners = new HashMap<>();
    private final Map<LeaseListener, NodeCollectionWatcher> leaseListeners = new HashMap<>();
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
    public boolean writeLeaseData(final CloudnamePath path, final String data) {
        final String zkPath = ZK_ROOT + path.join('/');
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
    public String readLeaseData(final CloudnamePath path) {
        if (path == null) {
            return null;
        }
        final String zkPath = ZK_ROOT + path.join('/');
        try {
            curator.sync().forPath(zkPath);
            final byte[] bytes = curator.getData().forPath(zkPath);
            return new String(bytes, Charsets.UTF_8);
        } catch (final Exception ex) {
            LOG.log(Level.WARNING, "Got exception reading client lease data at " + path, ex);
        }
        return null;
    }

    private CloudnamePath toCloudnamePath(final String zkPath) {
        final String clientPath = zkPath.substring(ZK_ROOT.length());
        final String[] elements = clientPath.split("/");
        return new CloudnamePath(elements);
    }

    @Override
    public void addLeaseCollectionListener(
            final CloudnamePath pathToObserve, final LeaseListener listener) {
        // Ideally the PathChildrenCache class in Curator would be used here to keep track of the
        // changes but it is ever so slightly broken and misses most of the watches that ZooKeeper
        // triggers, ignores the mzxid on the nodes and generally makes a mess of things. Enter
        // custom code.
        final String zkPath = ZK_ROOT + pathToObserve.join('/');
        try {
            curator.createContainers(zkPath);
            final NodeCollectionWatcher watcher = new NodeCollectionWatcher(
                    curator.getZookeeperClient().getZooKeeper(),
                    zkPath,
                    new NodeWatcherListener() {

                        @Override
                        public void nodeCreated(final String path, final String data) {
                            listener.leaseCreated(toCloudnamePath(path), data);
                        }

                        @Override
                        public void dataChanged(final String path, final String data) {
                            listener.dataChanged(toCloudnamePath(path), data);
                        }

                        @Override
                        public void nodeRemoved(final String path) {
                            listener.leaseRemoved(toCloudnamePath(path));
                        }
                    });

            synchronized (syncObject) {
                collectionListeners.put(listener, watcher);
            }
        } catch (final Exception exception) {
            LOG.log(Level.WARNING, "Got exception when creating node watcher", exception);
        }
    }

    @Override
    public void addLeaseListener(final CloudnamePath leaseToObserve, final LeaseListener listener) {
        try {
            final String parentPath = ZK_ROOT + leaseToObserve.getParent().join('/');
            final String fullPath = ZK_ROOT + leaseToObserve.join('/');
            curator.createContainers(parentPath);
            final NodeCollectionWatcher watcher = new NodeCollectionWatcher(
                    curator.getZookeeperClient().getZooKeeper(),
                    parentPath,
                    new NodeWatcherListener() {

                        @Override
                        public void nodeCreated(final String path, final String data) {
                            if (path.equals(fullPath)) {
                                listener.leaseCreated(toCloudnamePath(path), data);
                            }
                        }

                        @Override
                        public void dataChanged(final String path, final String data) {
                            if (path.equals(fullPath)) {
                                listener.dataChanged(toCloudnamePath(path), data);
                            }
                        }

                        @Override
                        public void nodeRemoved(final String path) {
                            if (path.equals(fullPath)) {
                                listener.leaseRemoved(toCloudnamePath(path));
                            }
                        }
                    });

            synchronized (syncObject) {
                leaseListeners.put(listener, watcher);
            }
        } catch (final Exception exception) {
            LOG.log(Level.WARNING, "Got exception when creating node watcher", exception);
        }
    }

    @Override
    public void removeLeaseListener(final LeaseListener listener) {
        synchronized (syncObject) {
            final NodeCollectionWatcher collectionWatcher = collectionListeners.get(listener);
            if (collectionWatcher != null) {
                collectionListeners.remove(listener);
                collectionWatcher.shutdown();
            }
            final NodeCollectionWatcher leaseWatcher = leaseListeners.get(listener);
            if (leaseWatcher != null) {
                leaseListeners.remove(listener);
                leaseWatcher.shutdown();
            }
        }
    }

    @Override
    public LeaseHandle createLease(
            final LeaseType type, final CloudnamePath path, final String data) {
        if (type == null || path == null || data == null) {
            return null;
        }

        final String zkPath = ZK_ROOT + path.join('/');
        try {
            curator.sync().forPath(zkPath);
            final Stat nodeStat = curator.checkExists().forPath(zkPath);
            if (nodeStat == null) {
                final CreateMode mode = (type == LeaseType.PERMANENT
                        ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL);

                final String returnedPath = curator.create()
                        .creatingParentContainersIfNeeded()
                        .withMode(mode)
                        .forPath(zkPath, data.getBytes(Charsets.UTF_8));

                if (returnedPath == null) {
                    LOG.warning("Could not create node for path " + path
                            + " - Curator returned null on create()");
                    return null;
                }
                return new LeaseHandle() {
                    private AtomicBoolean closed = new AtomicBoolean(false);

                    @Override
                    public boolean writeData(final String data) {
                        if (closed.get()) {
                            LOG.info("Attempt to write data to closed leased handle " + data);
                            return false;
                        }
                        return writeLeaseData(path, data);
                    }

                    @Override
                    public CloudnamePath getLeasePath() {
                        if (closed.get()) {
                            return null;
                        }
                        return path;
                    }

                    @Override
                    public void close() throws IOException {
                        if (type == LeaseType.PERMANENT || closed.get()) {
                            return;
                        }
                        try {
                            curator.delete().forPath(zkPath);
                            closed.set(true);
                        } catch (final Exception ex) {
                            throw new IOException(ex);
                        }
                    }
                };
            }

            LOG.log(Level.INFO, "Attempt to create node at " + path
                    + " with data " + data + " but it already exists");

        } catch (final Exception ex) {
            LOG.log(Level.WARNING, "Got exception creating parent container for lease"
                    + " for lease " + path + " with data " + data, ex);
        }
        return null;
    }

    @Override
    public boolean removeLease(final CloudnamePath path) {
        final String zkPath = ZK_ROOT + path.join('/');
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
            LOG.log(Level.WARNING, "Got error removing node for lease " + path, ex);
            return false;
        }
    }

    @Override
    public void close() {
        synchronized (syncObject) {
            collectionListeners.values().forEach(NodeCollectionWatcher::shutdown);
            collectionListeners.clear();
            leaseListeners.values().forEach(NodeCollectionWatcher::shutdown);
            leaseListeners.clear();
        }
    }
}
