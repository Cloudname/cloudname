package org.cloudname.zk;

import org.apache.zookeeper.ZooKeeper;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;


public class ZkObjectHandler {
    private ZooKeeper zooKeeper = null;
    private final Object zooKeeperMonitor = new Object();

    private final Set<ConnectionStateChanged> registerredCallbacks =
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
            for (ConnectionStateChanged connectionStateChanged : registerredCallbacks) {
                connectionStateChanged.connectionUp();
            }
        }
    }

    public void connectionDown() {
        isConnected.set(false);
        synchronized (callbacksMonitor) {
            for (ConnectionStateChanged connectionStateChanged : registerredCallbacks) {
                connectionStateChanged.connectionDown();
            }
        }
    }

    public class Client {
        public ZooKeeper getZookeeper() {
            synchronized (zooKeeperMonitor) {
                return zooKeeper;
            }
        }

        public boolean isConnected() {
            return isConnected.get();
        }

        /**
         * Register a callback.
         * @param connectionStateChanged
         * @return true if this is a new callback.
         */
        public boolean registerListener(ConnectionStateChanged connectionStateChanged) {
            synchronized (callbacksMonitor) {
                return registerredCallbacks.add(connectionStateChanged);
            }
        }

        /**
         * Deregister a callback.
         * @param connectionStateChanged
         * @return true if the callback was registered.
         */
        public boolean deregisterListener(ConnectionStateChanged connectionStateChanged) {
            synchronized (callbacksMonitor) {
                return registerredCallbacks.remove(connectionStateChanged);
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
            if (zooKeeper != null) {
                try {
                    zooKeeper.close();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }
}