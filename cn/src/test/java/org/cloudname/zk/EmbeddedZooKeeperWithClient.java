package org.cloudname.zk;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.cloudname.testtools.Net;
import org.cloudname.testtools.zookeeper.EmbeddedZooKeeper;
import org.junit.rules.TemporaryFolder;

/**
 * Utility class to start an {@link EmbeddedZooKeeper} instance. This is to be
 * reused in tests. Create an instance of this class as field, call its setup
 * method in a @Before annotated method, and its close method in a @After
 * annotated method.
 *
 * @author neumayer
 */
public class EmbeddedZooKeeperWithClient {

    private static Logger log = Logger.getLogger(EmbeddedZooKeeperWithClient.class.getName());

    private EmbeddedZooKeeper ezk;
    private ZooKeeper zk;
    private int zkPort;

    /**
     * Set up an embedded ZooKeeper instance backed by a temporary directory.
     * The setup procedure also allocates a port that is free for the ZooKeeper
     * server so that you should be able to run multiple instances of this
     * class.
     *
     * @param temp
     *            directory as created in the test class
     */
    public void setup(final TemporaryFolder temp) throws Exception {
        final File rootDir = temp.newFolder("zklock-test");
        zkPort = Net.getFreePort();

        log.info("EmbeddedZooKeeper rootDir=" + rootDir.getCanonicalPath() + ", port=" + zkPort);

        // Set up and initialize the embedded ZooKeeper
        ezk = new EmbeddedZooKeeper(rootDir, zkPort);
        ezk.init();

        // Set up a zookeeper client that we can use for inspection
        final CountDownLatch connectedLatch = new CountDownLatch(1);

        zk = new ZooKeeper("localhost:" + zkPort, 1000, new Watcher() {
            @Override
            public void process(final WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    connectedLatch.countDown();
                }
            }
        });
        connectedLatch.await();

        System.out.println("ZooKeeper port is " + zkPort);
    }

    public int getZkPort() {
        return zkPort;
    }

    public void close() throws Exception {
        zk.close();
    }

    public ZooKeeper getZk() {
        return zk;
    }
}
