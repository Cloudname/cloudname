package org.cloudname;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import org.apache.zookeeper.ZooKeeper;
import org.cloudname.testtools.Net;
import org.cloudname.testtools.zookeeper.EmbeddedZooKeeper;
import org.cloudname.zk.ZkCloudname;


/**
 * Helper class for bootstrapping cloudname for unit-tests. It also exposes the ZooKeeper instance.
 * @author @author dybdahl
 */
public class CloudnameTestBootstrapper {


    private static final Logger LOGGER = Logger.getLogger(CloudnameTestBootstrapper.class.getName());
    private EmbeddedZooKeeper embeddedZooKeeper;
    private ZooKeeper zooKeeper;
    private Cloudname cloudname;
    private File rootDir;
    
    public CloudnameTestBootstrapper(File rootDir) {
       this.rootDir = rootDir;
    }

    public void init() throws Exception, CloudnameException {
        int zookeeperPort = Net.getFreePort();

        LOGGER.info("EmbeddedZooKeeper rootDir=" + rootDir.getCanonicalPath()
                + ", port=" + zookeeperPort
        );

        // Set up and initialize the embedded ZooKeeper
        embeddedZooKeeper = new EmbeddedZooKeeper(rootDir, zookeeperPort);
        embeddedZooKeeper.init();

        // Set up a zookeeper client that we can use for inspection
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        zooKeeper = new ZooKeeper("localhost:" + zookeeperPort, 1000, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connectedLatch.countDown();
                }
            }
        });
        connectedLatch.await();
        cloudname = new ZkCloudname.Builder().setConnectString("localhost:" + zookeeperPort).build().connect();
    }
    
    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }
    
    public Cloudname getCloudname() {
        return cloudname;
    }
}
