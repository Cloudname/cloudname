package org.cloudname.backends.zookeeper;

/**
 * Listener interface for node change events
 *
 * @author stalehd@gmail.com
 */
public interface NodeWatcherListener {
    /**
     * A node is created. Note that rapid changes with create, data update (and even
     * create + delete + create + data change might yield just one create notification.
     *
     * @param zkPath path to node
     * @param data   data of node
     */
    void nodeCreated(final String zkPath, final String data);

    /**
     * Data on a node is changed. Note that you might not get data change notifications
     * for nodes that are created and updated within a short time span, only a create
     * notification.
     * Nodes that are created, deleted, then recreated will also generate this event, even if
     * the data is unchanged.
     *
     * @param zkPath path of node
     * @param data   data of node
     */
    void dataChanged(final String zkPath, final String data);

    /**
     * Node is removed.
     *
     * @param zkPath Path of the node that is removed.
     */
    void nodeRemoved(final String zkPath);
}


