package org.cloudname.zk;

import org.apache.zookeeper.ZooKeeper;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;


public class ZkObjectHandler {
    private ZooKeeper zooKeeper = null;
    private final Object zooKeeperMonitor = new Object();

    private final Set<ConnectionStateChanged> registeredCallbacks =
            new HashSet<ConnectionStateChanged>();
    private final Object callbacksMonitor = new Object();

    private final AtomicBoolean isConnected = new AtomicBoolean(false);

    public ZkObjectHandler(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public interface ConnectionStateChanged {
        void connectionUp();
        void connectionDown();
    }

    public void connectionUp() {
        isConnected.set(true);
        synchronized (callbacksMonitor) {
            for (ConnectionStateChanged connectionStateChanged : registeredCallbacks) {
                connectionStateChanged.connectionUp();
            }
        }
    }

    public void connectionDown() {
        isConnected.set(false);
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
    public Client getClient() {
        return new Client();
    }

    public void setZooKeeper(final ZooKeeper zooKeeper) {
        synchronized (zooKeeperMonitor) {
            this.zooKeeper = zooKeeper;
        }
    }

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
}