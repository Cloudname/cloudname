package org.cloudname.testtools.zookeeper;

import org.cloudname.testtools.Net;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import org.junit.*;
import static org.junit.Assert.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.logging.Logger;
import java.util.concurrent.CountDownLatch;
import java.nio.charset.Charset;

/**
 * Unit test for EmbeddedZooKeeper.
 *
 * @author borud
 */
public class EmbeddedZooKeeperTest {
    private static final Logger log = Logger.getLogger(EmbeddedZooKeeperTest.class.getName());
    private static final int SESSION_TIMEOUT = 5000;

    @Rule public TemporaryFolder temp = new TemporaryFolder();

    /**
     * Watcher that just looks for connections to be established.
     */
    private static class TestWatcher implements Watcher {
        private static final CountDownLatch connectedSignal = new CountDownLatch(1);

        @Override
        public void process(WatchedEvent event) {
            if (event.getState() == KeeperState.SyncConnected) {
                log.info("Got SyncConnected event");
                connectedSignal.countDown();
            }
        }

        public void waitForConnect() throws InterruptedException {
            log.info("Waiting for connectedSignal");
            connectedSignal.await();
            log.info("Got connectedSignal");
        }
    }

    /**
     * Fire up an EmbeddedZooKeeper instance, then try to connect to
     * it and create a znode.
     */
    @Test public void simpleTest() throws Exception {
        File rootDir = temp.newFolder("zk-test");
        int port = Net.getFreePort();

        log.info("EmbeddedZooKeeper rootDir=" + rootDir.getCanonicalPath()
                 + ", port=" + port
        );

        // Set up and initialize the embedded ZooKeeper
        EmbeddedZooKeeper ezk = new EmbeddedZooKeeper(rootDir, port);
        ezk.init();

        // Set up a TestWatcher to tell us when we are connected
        TestWatcher watcher = new TestWatcher();

        // try to connect a client and create a node
        ZooKeeper zk = new ZooKeeper(ezk.getClientConnectionString(),
                                     SESSION_TIMEOUT,
                                     watcher);

        // Block until we have a connection
        watcher.waitForConnect();

        // Create a node (with a slightly unique name)
        String nodeName = "/simpleTestNode" + System.currentTimeMillis();
        String nodeData = "this is the node data";
        String createdPath = zk.create(nodeName,
                                       nodeData.getBytes("UTF-8"),
                                       Ids.OPEN_ACL_UNSAFE,
                                       CreateMode.PERSISTENT);

        log.info("Created node " + nodeName);

        // Make sure the node is there
        Stat stat = zk.exists(nodeName, false);
        assertNotNull(stat);

        log.info("Stat : " + stat.toString());

        // Read the node
        byte[] data = zk.getData(nodeName,
                                 false,
                                 stat);

        // Convert back to String
        String fetchedData = new String(data, Charset.forName("UTF-8"));
        assertEquals(nodeData, fetchedData);

        log.info("Read " + nodeName + " and got '" + fetchedData + "'");

        zk.close();
        ezk.shutdown();
    }
}