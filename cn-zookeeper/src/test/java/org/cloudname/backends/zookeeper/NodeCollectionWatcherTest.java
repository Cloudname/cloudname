package org.cloudname.backends.zookeeper;

import org.apache.curator.CuratorConnectionLossException;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

/**
 * Test the node watching mechanism.
 */
public class NodeCollectionWatcherTest {
    private static TestingCluster zkServer;
    private static CuratorFramework curator;
    private static ZooKeeper zooKeeper;

    @BeforeClass
    public static void setUp() throws Exception {
        zkServer = new TestingCluster(3);
        zkServer.start();
        final RetryPolicy retryPolicy = new RetryUntilElapsed(60000, 100);
        curator = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), retryPolicy);
        curator.start();
        curator.blockUntilConnected(10, TimeUnit.SECONDS);
        zooKeeper = curator.getZookeeperClient().getZooKeeper();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        zkServer.close();
    }

    private final AtomicInteger counter = new AtomicInteger(0);

    private byte[] getData() {
        return ("" + counter.incrementAndGet()).getBytes(Charset.defaultCharset());
    }

    /**
     * A custom listener that counts and counts down notifications.
     */
    private class ListenerCounter implements NodeWatcherListener {
        // Then a few counters to check the number of events
        public AtomicInteger createCount = new AtomicInteger(0);
        public AtomicInteger dataCount = new AtomicInteger(0);
        public AtomicInteger removeCount = new AtomicInteger(0);
        public CountDownLatch createLatch;
        public CountDownLatch dataLatch;
        public CountDownLatch removeLatch;

        public ListenerCounter(final int createLatchCount, final int dataLatchCount, final int removeLatchCount) {
            createLatch = new CountDownLatch(createLatchCount);
            dataLatch = new CountDownLatch(dataLatchCount);
            removeLatch = new CountDownLatch(removeLatchCount);
        }

        @Override
        public void nodeCreated(String zkPath, String data) {
            createCount.incrementAndGet();
            createLatch.countDown();
        }

        @Override
        public void dataChanged(String zkPath, String data) {
            dataCount.incrementAndGet();
            dataLatch.countDown();
        }

        @Override
        public void nodeRemoved(String zkPath) {
            removeCount.incrementAndGet();
            removeLatch.countDown();
        }
    }

    @Test
    public void sequentialNotifications() throws Exception {
        final int maxPropagationTime = 4;

        final String pathPrefix = "/foo/slow";
        curator.create().creatingParentsIfNeeded().forPath(pathPrefix);

        final ListenerCounter listener = new ListenerCounter(1, 1, 1);

        final NodeCollectionWatcher nodeCollectionWatcher = new NodeCollectionWatcher(zooKeeper, pathPrefix, listener);

        // Create should trigger create notification (and no other notification)
        curator.create().forPath(pathPrefix + "/node1", getData());
        assertTrue(listener.createLatch.await(maxPropagationTime, TimeUnit.MILLISECONDS));
        assertThat(listener.createCount.get(), is(1));
        assertThat(listener.dataCount.get(), is(0));
        assertThat(listener.removeCount.get(), is(0));

        // Data change should trigger the data notification (and no other notification)
        curator.setData().forPath(pathPrefix + "/node1", getData());
        assertTrue(listener.dataLatch.await(maxPropagationTime, TimeUnit.MILLISECONDS));
        assertThat(listener.createCount.get(), is(1));
        assertThat(listener.dataCount.get(), is(1));
        assertThat(listener.removeCount.get(), is(0));

        // Delete should trigger the remove notification (and no other notification)
        curator.delete().forPath(pathPrefix + "/node1");
        assertTrue(listener.removeLatch.await(maxPropagationTime, TimeUnit.MILLISECONDS));
        assertThat(listener.createCount.get(), is(1));
        assertThat(listener.dataCount.get(), is(1));
        assertThat(listener.removeCount.get(), is(1));

        nodeCollectionWatcher.shutdown();

        // Ensure that there are no notifications when the watcher shuts down
        curator.create().forPath(pathPrefix + "node_9", getData());
        Thread.sleep(maxPropagationTime);
        assertThat(listener.createCount.get(), is(1));
        assertThat(listener.dataCount.get(), is(1));
        assertThat(listener.removeCount.get(), is(1));

        curator.setData().forPath(pathPrefix + "node_9", getData());
        Thread.sleep(maxPropagationTime);
        assertThat(listener.createCount.get(), is(1));
        assertThat(listener.dataCount.get(), is(1));
        assertThat(listener.removeCount.get(), is(1));

        curator.delete().forPath(pathPrefix + "node_9");
        Thread.sleep(maxPropagationTime);
        assertThat(listener.createCount.get(), is(1));
        assertThat(listener.dataCount.get(), is(1));
        assertThat(listener.removeCount.get(), is(1));
    }

    /**
     * Make rapid changes to ZooKeeper. The changes (most likely) won't be caught by the
     * watcher events but must be generated by the class itself. Ensure the correct number
     * of notifications is generated.
     */
    @Test
    public void rapidChanges() throws Exception {
        final int maxPropagationTime = 100;

        final String pathPrefix = "/foo/rapido";

        curator.create().creatingParentsIfNeeded().forPath(pathPrefix);

        final int numNodes = 50;
        final ListenerCounter listener = new ListenerCounter(numNodes, 0, numNodes);

        final NodeCollectionWatcher nodeCollectionWatcher = new NodeCollectionWatcher(zooKeeper, pathPrefix, listener);
        // Create all of the nodes at once
        for (int i = 0; i < numNodes; i++) {
            curator.create().forPath(pathPrefix + "/node" + i, getData());
        }
        assertTrue(listener.createLatch.await(maxPropagationTime, TimeUnit.MILLISECONDS));
        assertThat(listener.createCount.get(), is(numNodes));
        assertThat(listener.dataCount.get(), is(0));
        assertThat(listener.removeCount.get(), is(0));

        // Repeat data test multiple times to ensure data changes are detected
        // repeatedly on the same nodes
        int total = 0;
        for (int j = 0; j < 5; j++) {
            listener.dataLatch = new CountDownLatch(numNodes);
            // Since there's a watch for every node all of the data changes should be detected
            for (int i = 0; i < numNodes; i++) {
                curator.setData().forPath(pathPrefix + "/node" + i, getData());
            }
            total += numNodes;
            assertTrue(listener.dataLatch.await(maxPropagationTime, TimeUnit.MILLISECONDS));
            assertThat(listener.createCount.get(), is(numNodes));
            assertThat(listener.dataCount.get(), is(total));
            assertThat(listener.removeCount.get(), is(0));
        }

        // Finally, remove everything in rapid succession
        // Create all of the nodes at once
        for (int i = 0; i < numNodes; i++) {
            curator.delete().forPath(pathPrefix + "/node" + i);
        }

        assertTrue(listener.removeLatch.await(maxPropagationTime, TimeUnit.MILLISECONDS));
        assertThat(listener.createCount.get(), is(numNodes));
        assertThat(listener.dataCount.get(), is(total));
        assertThat(listener.removeCount.get(), is(numNodes));

        nodeCollectionWatcher.shutdown();
    }

    /**
     * Emulate a network partition by killing off two out of three ZooKeeper instances
     * and check the output. Set the system property NodeWatcher.SlowTests to "ok" to enable
     * it. The test itself can be quite slow depending on what Curator is connected to. If
     * Curator uses one of the servers that are killed it will try a reconnect and the whole
     * test might take up to 120-180 seconds to complete.
     */
    @Test
    public void networkPartitionTest() throws Exception {
        assumeThat(System.getProperty("NodeCollectionWatcher.SlowTests"), is("ok"));

        final int maxPropagationTime = 10;

        final String pathPrefix = "/foo/partition";
        curator.create().creatingParentsIfNeeded().forPath(pathPrefix);

        final int nodeCount = 10;

        final ListenerCounter listener = new ListenerCounter(nodeCount, nodeCount, nodeCount);

        final NodeCollectionWatcher nodeCollectionWatcher = new NodeCollectionWatcher(zooKeeper, pathPrefix, listener);

        // Create a few nodes to set the initial state
        for (int i = 0; i < nodeCount; i++) {
            curator.create().forPath(pathPrefix + "/node" + i, getData());
        }
        assertTrue(listener.createLatch.await(maxPropagationTime, TimeUnit.MILLISECONDS));
        assertThat(listener.createCount.get(), is(nodeCount));
        assertThat(listener.removeCount.get(), is(0));
        assertThat(listener.dataCount.get(), is(0));

        final InstanceSpec firstInstance = zkServer.findConnectionInstance(zooKeeper);
        zkServer.killServer(firstInstance);

        listener.createLatch = new CountDownLatch(1);
        // Client should reconnect to one of the two remaining
        curator.create().forPath(pathPrefix + "/stillalive", getData());
        // Wait for the notification to go through. This could take some time since there's
        // reconnects and all sorts of magic happening under the hood
        assertTrue(listener.createLatch.await(10, TimeUnit.SECONDS));
        assertThat(listener.createCount.get(), is(nodeCount + 1));
        assertThat(listener.removeCount.get(), is(0));
        assertThat(listener.dataCount.get(), is(0));

        // Kill the 2nd server. The cluster won't have a quorum now
        final InstanceSpec secondInstance = zkServer.findConnectionInstance(zooKeeper);
        assertThat(firstInstance, is(not(secondInstance)));
        zkServer.killServer(secondInstance);

        boolean retry;
        do {
            System.out.println("Checking node with Curator... This might take a while...");
            try {
                final Stat stat = curator.checkExists().forPath(pathPrefix);
                retry = false;
                assertThat(stat, is(notNullValue()));
            } catch (CuratorConnectionLossException ex) {
                System.out.println("Missing connection. Retrying");
                retry = true;
            }
        } while (retry);

        zkServer.restartServer(firstInstance);
        zkServer.restartServer(secondInstance);
        listener.createLatch = new CountDownLatch(1);

        System.out.println("Creating node via Curator... This might take a while...");
        curator.create().forPath(pathPrefix + "/imback", getData());

        assertTrue(listener.createLatch.await(maxPropagationTime, TimeUnit.MILLISECONDS));
        assertThat(listener.createCount.get(), is(nodeCount + 2));
        assertThat(listener.removeCount.get(), is(0));
        assertThat(listener.dataCount.get(), is(0));

        // Ensure data notifications are propagated after a failure
        for (int i = 0; i < nodeCount; i++) {
            final Stat stat = curator.setData().forPath(pathPrefix + "/node" + i, getData());
            assertThat(stat, is(notNullValue()));
        }
        assertTrue(listener.dataLatch.await(maxPropagationTime, TimeUnit.MILLISECONDS));
        assertThat(listener.createCount.get(), is(nodeCount + 2));
        assertThat(listener.removeCount.get(), is(0));
        assertThat(listener.dataCount.get(), is(nodeCount));

        // ..and remove notifications are sent
        for (int i = 0; i < nodeCount; i++) {
            curator.delete().forPath(pathPrefix + "/node" + i);
        }
        assertTrue(listener.removeLatch.await(maxPropagationTime, TimeUnit.MILLISECONDS));
        assertThat(listener.createCount.get(), is(nodeCount + 2));
        assertThat(listener.removeCount.get(), is(nodeCount));
        assertThat(listener.dataCount.get(), is(nodeCount));

        nodeCollectionWatcher.shutdown();

    }

    /**
     * Be a misbehaving client and throw exceptions in the listners. Ensure the watcher still works
     * afterwards.
     */
    @Test
    public void misbehavingClient() throws Exception {
        final int propagationTime = 5;

        final AtomicBoolean triggerExceptions = new AtomicBoolean(false);
        final CountDownLatch createLatch = new CountDownLatch(1);
        final CountDownLatch dataLatch = new CountDownLatch(1);
        final CountDownLatch removeLatch = new CountDownLatch(1);

        final NodeWatcherListener listener = new NodeWatcherListener() {
            @Override
            public void nodeCreated(String zkPath, String data) {
                if (triggerExceptions.get()) {
                    throw new RuntimeException("boo!");
                }
                createLatch.countDown();
            }

            @Override
            public void dataChanged(String zkPath, String data) {
                if (triggerExceptions.get()) {
                    throw new RuntimeException("boo!");
                }
                dataLatch.countDown();
            }

            @Override
            public void nodeRemoved(String zkPath) {
                if (triggerExceptions.get()) {
                    throw new RuntimeException("boo!");
                }
                removeLatch.countDown();
            }
        };

        final String pathPrefix = "/foo/misbehaving";

        curator.create().creatingParentsIfNeeded().forPath(pathPrefix);

        final NodeCollectionWatcher nodeCollectionWatcher = new NodeCollectionWatcher(zooKeeper, pathPrefix, listener);

        triggerExceptions.set(true);
        curator.create().forPath(pathPrefix + "/first", getData());
        Thread.sleep(propagationTime);
        curator.setData().forPath(pathPrefix + "/first", getData());
        Thread.sleep(propagationTime);
        curator.delete().forPath(pathPrefix + "/first");
        Thread.sleep(propagationTime);

        // Now create a node but without setting the data field.
        triggerExceptions.set(false);
        curator.create().forPath(pathPrefix + "/second");
        assertTrue(createLatch.await(propagationTime, TimeUnit.MILLISECONDS));
        curator.setData().forPath(pathPrefix + "/second", getData());
        assertTrue(dataLatch.await(propagationTime, TimeUnit.MILLISECONDS));
        curator.delete().forPath(pathPrefix + "/second");
        assertTrue(removeLatch.await(propagationTime, TimeUnit.MILLISECONDS));

        nodeCollectionWatcher.shutdown();
    }
}
