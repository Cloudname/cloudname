package org.cloudname.backends.zookeeper;

import com.google.common.base.Charsets;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Monitor a set of child nodes for changes. Needs to do this with the ZooKeeper API since
 * Curator doesn't provide the necessary interface and the PathChildrenCache is best effort
 * (and not even a very good effort)
 *
 * <p>Watches are kept as usual and the mzxid for each node is kept. If that changes between
 * watches it mens we've missed an event and the appropriate event is generated to the
 * listener.
 *
 * <p>Note that this class only watches for changes one level down. Changes in children aren't
 * monitored. The path must exist beforehand.
 *
 * @author stalehd@gmail.com
 */
public class NodeCollectionWatcher {
    private static final Logger LOG = Logger.getLogger(NodeCollectionWatcher.class.getName());

    private final Map<String, Long> childMzxid = new HashMap<>();
    private final Object syncObject = new Object();

    private final ZooKeeper zk;
    private final String pathToWatch;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final NodeWatcherListener listener;


    /**
     * Create and start the collection watcher. The supplied @link{ZooKeeper} instance is used to
     * read nodes from the path <pre>pathToWatch</pre>. Changes are communicated with the
     * supplied @link{NodeWatcherListener}.
     */
    public NodeCollectionWatcher(
            final ZooKeeper zk, final String pathToWatch, final NodeWatcherListener listener) {
        this.pathToWatch = pathToWatch;
        this.zk = zk;
        this.listener = listener;
        readChildNodes();
    }

    /**
     * Shut down watchers. The listener won't get notified of changes after it has been shut down.
     */
    public void shutdown() {
        shuttingDown.set(true);
    }

    /**
     * Watcher for node collections. Set by getChildren().
     */
    private final Watcher nodeCollectionWatcher = (watchedEvent) -> {
        switch (watchedEvent.getType()) {
            case NodeChildrenChanged:
                // Child values have changed, read children, generate events
                readChildNodes();
                break;
            case None:
                // Some zookeeper event. Watches might not apply anymore. Reapply.
                switch (watchedEvent.getState()) {
                    case ConnectedReadOnly:
                        LOG.severe("Connected to readonly cluster");
                        // Connected to a cluster without quorum. Nodes might not be
                        // correct but re-read the nodes.
                        readChildNodes();
                        break;
                    case SyncConnected:
                        LOG.info("Connected to cluster");
                        // (re-)Connected to the cluster. Nodes must be re-read. Discard
                        // those that aren't found, keep unchanged ones.
                        readChildNodes();
                        break;
                    case Disconnected:
                        // Disconnected from the cluster. The nodes might not be
                        // up to date (but a reconnect might solve the issue)
                        LOG.log(Level.WARNING, "Disconnected from zk cluster");
                        break;
                    case Expired:
                        // Session has expired. Nodes are no longer available
                        removeAllChildNodes();
                        break;
                    default:
                        break;
                }
                break;
            default:
                break;
        }
    };

    /**
     * A watcher for the child nodes (set via getData()).
     */
    private final Watcher changeWatcher = (watchedEvent) -> {
        if (shuttingDown.get()) {
            return;
        }
        switch (watchedEvent.getType()) {
            case NodeDeleted:
                removeChildNode(watchedEvent.getPath());
                break;
            case NodeDataChanged:
                processNode(watchedEvent.getPath());
                break;
            default:
                break;
        }
    };

    /**
     * Remove all nodes.
     */
    private void removeAllChildNodes() {
        System.out.println("Remove all child nodes");
        final Set<String> nodesToRemove = new HashSet<>();
        synchronized (syncObject) {
            nodesToRemove.addAll(childMzxid.keySet());
        }
        for (final String node : nodesToRemove) {
            removeChildNode(node);
        }
    }

    /**
     * Read nodes from ZooKeeper, generating events as necessary. If a node is missing from the
     * result it will generate a remove notification, ditto with new nodes and changes in nodes.
     */
    private void readChildNodes() {
        try {
            final List<String> childNodes = zk.getChildren(pathToWatch, nodeCollectionWatcher);
            final Set<String> childrenToDelete = new HashSet<>();
            synchronized (syncObject) {
                childrenToDelete.addAll(childMzxid.keySet());
            }
            for (final String nodeName : childNodes) {
                processNode(pathToWatch + "/" + nodeName);
                childrenToDelete.remove(pathToWatch + "/" + nodeName);
            }
            for (final String nodePath : childrenToDelete) {
                removeChildNode(nodePath);
            }
        } catch (final KeeperException.ConnectionLossException e) {
            // We've been disconnected. Let the watcher deal with it
            if (!shuttingDown.get()) {
                LOG.info("Lost connection to ZooKeeper while reading child nodes.");
            }
        } catch (final KeeperException.NoNodeException e) {
            // Node has been removed. Ignore the error?
            removeChildNode(e.getPath());
        } catch (final KeeperException | InterruptedException e) {
            LOG.log(Level.WARNING, "Got exception reading child nodes", e);
        }
    }

    /**
     * Add a node, generate create or data change notification if needed.
     */
    private void processNode(final String nodePath) {
        if (shuttingDown.get()) {
            return;
        }
        try {
            final Stat stat = new Stat();
            final byte[] nodeData = zk.getData(nodePath, changeWatcher, stat);
            final String data = new String(nodeData, Charsets.UTF_8);
            synchronized (syncObject) {
                if (!childMzxid.containsKey(nodePath)) {
                    childMzxid.put(nodePath, stat.getMzxid());
                    generateCreateEvent(nodePath, data);
                    return;
                }
                final Long zxid = childMzxid.get(nodePath);
                if (zxid != stat.getMzxid()) {
                    // the data have changed. Generate event
                    childMzxid.put(nodePath, stat.getMzxid());
                    generateDataChangeEvent(nodePath, data);
                }
            }
        } catch (final KeeperException.ConnectionLossException e) {
            // We've been disconnected. Let the watcher deal with it
            if (!shuttingDown.get()) {
                LOG.info("Lost connection to ZooKeeper while reading child nodes.");
            }
        } catch (final KeeperException.NoNodeException e) {
            removeChildNode(e.getPath());
            // Node has been removed before we got to do anything. Ignore error?
        } catch (final KeeperException | InterruptedException e) {
            LOG.log(Level.WARNING, "Got exception adding child node with path " + nodePath, e);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "Pooop!", ex);
        }
    }

    /**
     * Remove node. Generate remove event if needed.
     */
    private void removeChildNode(final String nodePath) {
        synchronized (syncObject) {
            if (childMzxid.containsKey(nodePath)) {
                childMzxid.remove(nodePath);
                generateRemoveEvent(nodePath);
            }
        }
    }

    /**
     * Invoke nodeCreated on listener.
     */
    private void generateCreateEvent(final String nodePath, final String data) {
        try {
            listener.nodeCreated(nodePath, data);
        } catch (final Exception exception) {
            LOG.log(Level.WARNING, "Got exception calling listener.nodeCreated", exception);
        }
    }

    /**
     * Invoke dataChanged on listener.
     */
    private void generateDataChangeEvent(final String nodePath, final String data) {
        try {
            listener.dataChanged(nodePath, data);
        } catch (final Exception exception) {
            LOG.log(Level.WARNING, "Got exception calling listener.dataChanged", exception);
        }
    }

    /**
     * Invoke nodeRemoved on listener.
     */
    private void generateRemoveEvent(final String nodePath) {
        try {
            listener.nodeRemoved(nodePath);
        } catch (final Exception exception) {
            LOG.log(Level.WARNING, "Got exception calling listener.nodeRemoved", exception);
        }
    }
}
