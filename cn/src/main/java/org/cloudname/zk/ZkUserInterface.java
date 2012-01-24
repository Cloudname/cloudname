package org.cloudname.zk;


import org.apache.zookeeper.ZooKeeper;

/**
 * Classes that uses ZooKeeper must implement this interface to stay in sync.
 * This is the preferred way to know if connection to ZooKeeper goes down.
 */
public interface ZkUserInterface {

    public void zooKeeperDown();

    public void newZooKeeperInstance(ZooKeeper zk);
}
