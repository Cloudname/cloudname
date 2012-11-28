package org.cloudname.zk;

import org.apache.zookeeper.ZooKeeper;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class that keeps an instance of zookeeper. It has a sub-class with read access and
 * a listener service.
 * @author dybdahl
 */
public class ZkObjectHandler {
    private ZooKeeper zooKeeper = null;
    private final Object zooKeeperMonitor = new Object();

    private final Set<ConnectionStateChanged> registeredCallbacks =
            new HashSet<ConnectionStateChanged>();
    private final Object callbacksMonitor = new Object();

    private final AtomicBoolean isConnected = new AtomicBoolean(true);

    /**
     * Constructor
     * @param zooKeeper first zooKeeper to use, should not be null.
     */
    public ZkObjectHandler(final ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    /**
     * Interface for notification of connection state changes.
     */
    public interface ConnectionStateChanged {
        void connectionUp();
        void connectionDown();
        void shutDown();
    }

    /**
     * Indicate that zookeeper connection is working by calling this method.
     */
    public void connectionUp() {
        boolean previous = isConnected.getAndSet(true);
        if (previous == true) { return; }
        synchronized (callbacksMonitor) {
            for (ConnectionStateChanged connectionStateChanged : registeredCallbacks) {
                connectionStateChanged.connectionUp();
            }
        }
    }

    /**
     * Indicate that zookeeper connection is broken by calling this method.
     */
    public void connectionDown() {
        boolean previous = isConnected.getAndSet(false);
        if (previous == false) { return; }
        synchronized (callbacksMonitor) {
            for (ConnectionStateChanged connectionStateChanged : registeredCallbacks) {
                connectionStateChanged.connectionDown();
            }
        }
    }

    /**
     * Every class using Zookeeper has an instance of this Client class
     * to check the connection and fetch the instance.
     */
    public class Client {
        public ZooKeeper getZookeeper() {
            synchronized (zooKeeperMonitor) {
                return zooKeeper;
            }
        }

        /**
         * Check if we are connected to Zookeeper
         * @return True if zkCloudname confirmed connection <1000ms ago.
         */
        public boolean isConnected() {
            return isConnected.get();
        }

        /**
         * Register a callback.
         * @param connectionStateChanged Callback to register
         * @return true if this is a new callback.
         */
        public boolean registerListener(ConnectionStateChanged connectionStateChanged) {
            synchronized (callbacksMonitor) {
                return registeredCallbacks.add(connectionStateChanged);
            }
        }

        /**
         * Deregister a callback.
         * @param connectionStateChanged Callback to deregister.
         * @return true if the callback was registered.
         */
        public boolean deregisterListener(ConnectionStateChanged connectionStateChanged) {
            synchronized (callbacksMonitor) {
                return registeredCallbacks.remove(connectionStateChanged);
            }
        }
    }

    /**
     * Returns client
     * @return client object.
     */
    public Client getClient() {
        return new Client();
    }

    /**
     * Update zooKeeper instance.
     * @param zooKeeper
     */
    public void setZooKeeper(final ZooKeeper zooKeeper) {
        synchronized (zooKeeperMonitor) {
            this.zooKeeper = zooKeeper;
        }
    }

    /**
     * Closes zooKeeper object.
     */
    public void close() {
        synchronized (zooKeeperMonitor) {
            if (zooKeeper == null) { return; }

            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    /**
     * Shut down all listeners.
     */
    public void shutdown() {
        synchronized (callbacksMonitor) {
            for (ConnectionStateChanged connectionStateChanged : registeredCallbacks) {
                connectionStateChanged.shutDown();
            }
        }
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            // ignore
        }
    }
}