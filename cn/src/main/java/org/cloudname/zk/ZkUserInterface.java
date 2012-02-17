package org.cloudname.zk;


import org.apache.zookeeper.ZooKeeper;

/**
 * Classes that uses ZooKeeper must implement this interface to stay in sync.
 * This is the preferred and only way to know if connection to ZooKeeper is up.
 */
public interface ZkUserInterface {

    /**
     * This indicate that the there are problems talking to the ZooKeeper instance. The system is still
     * trying to reconnect.
     */
    public void zooKeeperDown();

    /**
     * The system managed to connect to ZooKeeper and sends the new ZooKeeper instance.
     */
    public void newZooKeeperInstance(ZooKeeper zk);

    /**
     * This is an event sent about every second as long as the connection to ZooKeeper is up. The clients can
     * do verification of state etc. It is just intended to make things simple.
     */
    public void timeEvent();
}
