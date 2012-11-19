package org.cloudname.zk;

import org.cloudname.*;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.junit.*;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import org.cloudname.testtools.Net;
import org.cloudname.testtools.zookeeper.EmbeddedZooKeeper;

import java.io.File;
import java.util.logging.Logger;

/**
 * Unit test for the ZkCloudname class.
 *
 * @author borud, dybdahl
 */
public class ZkCloudnameTest {
    private static final Logger LOG = Logger.getLogger(ZkCloudnameTest.class.getName());

    private ZooKeeper zk;
    private int zkport;

    @Rule public TemporaryFolder temp = new TemporaryFolder();

    /**
     * Set up an embedded ZooKeeper instance backed by a temporary
     * directory.  The setup procedure also allocates a port that is
     * free for the ZooKeeper server so that you should be able to run
     * multiple instances of this test.
     */
    @Before
    public void setup() throws Exception {
        File rootDir = temp.newFolder("zk-test");
        zkport = Net.getFreePort();

        LOG.info("EmbeddedZooKeeper rootDir=" + rootDir.getCanonicalPath() + ", port=" + zkport
        );

        // Set up and initialize the embedded ZooKeeper
        final EmbeddedZooKeeper ezk = new EmbeddedZooKeeper(rootDir, zkport);
        ezk.init();

        // Set up a zookeeper client that we can use for inspection
        final CountDownLatch connectedLatch = new CountDownLatch(1);

        zk = new ZooKeeper("localhost:" + zkport, 1000, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    connectedLatch.countDown();
                }
            }
        });
        connectedLatch.await();

        LOG.info("ZooKeeper port is " + zkport);
    }

    @After
    public void tearDown() throws Exception {
        zk.close();
    }

    /**
     * Tests that the time-out mechanism on connecting to ZooKeeper works.
     */
    @Test
    public void testTimeout() throws IOException, InterruptedException {
        int deadPort = Net.getFreePort();
        try {
            new ZkCloudname.Builder().setConnectString("localhost:" + deadPort).build()
                    .connectWithTimeout(1000, TimeUnit.NANOSECONDS);
            fail("Expected time-out exception.");
        } catch (CloudnameException e) {
            // Expected.
        }
    }

    /**
     * A relatively simple voyage through a typical lifecycle.
     */
    @Test
    public void testSimple() throws Exception {
        final Coordinate c = Coordinate.parse("1.service.user.cell");
        final ZkCloudname cn = makeLocalZkCloudname();

        assertFalse(pathExists("/cn/cell/user/service/1"));
        cn.createCoordinate(c);

        // Coordinate should exist, but no status node
        assertTrue(pathExists("/cn/cell/user/service/1"));
        assertTrue(pathExists("/cn/cell/user/service/1/config"));
        assertFalse(pathExists("/cn/cell/user/service/1/status"));

        // Claiming the coordinate creates the status node
        final ServiceHandle handle = cn.claim(c);
        assertTrue(handle.waitForCoordinateOkSeconds(3));
        assertNotNull(handle);
        final CountDownLatch latch = new CountDownLatch(1);
        handle.registerCoordinateListener(new CoordinateListener() {

            @Override
            public void onCoordinateEvent(Event event, String message) {
                if (event == Event.COORDINATE_OK) {
                    latch.countDown();
                }
            }
        });
        assertTrue(latch.await(2, TimeUnit.SECONDS));

        final CountDownLatch configLatch1 = new CountDownLatch(1);
        final CountDownLatch configLatch2 = new CountDownLatch(2);
        final StringBuilder buffer = new StringBuilder();
        handle.registerConfigListener(new ConfigListener() {
            @Override
            public void onConfigEvent(Event event, String data) {
                buffer.append(data);
                configLatch1.countDown();
                configLatch2.countDown();
            }
        });
        assertTrue(configLatch1.await(5, TimeUnit.SECONDS));
        assertEquals(buffer.toString(), "");
        zk.setData("/cn/cell/user/service/1/config", "hello".getBytes(), -1);
        assertTrue(configLatch2.await(5, TimeUnit.SECONDS));
        assertEquals(buffer.toString(), "hello");

        assertTrue(pathExists("/cn/cell/user/service/1/status"));

        List<String> nodes = new ArrayList<String>();
        cn.listRecursively(nodes);
        assertEquals(2, nodes.size());
        assertEquals(nodes.get(0), "/cn/cell/user/service/1/config");
        assertEquals(nodes.get(1), "/cn/cell/user/service/1/status");

        // Try to set the status to something else
        String msg = "Hamster getting quite eager now";
        handle.setStatus(new ServiceStatus(ServiceState.STARTING,msg));
        ServiceStatus status = cn.getStatus(c);
        assertEquals(msg, status.getMessage());
        assertSame(ServiceState.STARTING, status.getState());

        // Publish two endpoints
        handle.putEndpoint(new Endpoint(c, "foo", "localhost", 1234, "http", null));
        handle.putEndpoint(new Endpoint(c, "bar", "localhost", 1235, "http", null));

        handle.setStatus(new ServiceStatus(ServiceState.RUNNING, msg));

        // Remove one of them
        handle.removeEndpoint("bar");

        List<Endpoint> endpointList = cn.getResolver().resolve("bar.1.service.user.cell");
        assertEquals(0, endpointList.size());

        endpointList = cn.getResolver().resolve("foo.1.service.user.cell");
        assertEquals(1, endpointList.size());
        Endpoint endpointFoo = endpointList.get(0);

        String fooData = endpointFoo.getName();
        assertEquals("foo", fooData);
        assertEquals("foo", endpointFoo.getName());
        assertEquals("localhost", endpointFoo.getHost());
        assertEquals(1234, endpointFoo.getPort());
        assertEquals("http", endpointFoo.getProtocol());
        assertNull(endpointFoo.getEndpointData());

        // Close handle just invalidates handle
        handle.close();

        // These nodes are ephemeral and will be cleaned out when we
        // call cn.releaseClaim(), but calling handle.releaseClaim() explicitly
        // cleans out the ephemeral nodes.
        assertFalse(pathExists("/cn/cell/user/service/1/status"));

        // Closing Cloudname instance disconnects the zk client
        // connection and thus should kill all ephemeral nodes.
        cn.close();

        // But the coordinate and its persistent subnodes should
        assertTrue(pathExists("/cn/cell/user/service/1"));
        assertFalse(pathExists("/cn/cell/user/service/1/endpoints"));
        assertTrue(pathExists("/cn/cell/user/service/1/config"));
    }

    /**
     * Claim non-existing coordinate
     */
    @Test
    public void testCoordinateNotFound() throws CloudnameException, InterruptedException {
        final Coordinate c = Coordinate.parse("3.service.user.cell");
        final Cloudname cn = makeLocalZkCloudname();

        final ExecutorService executor = Executors.newCachedThreadPool();
        final Callable<Object> task = new Callable<Object>() {
            public Object call() throws InterruptedException {
                return cn.claim(c);
            }
        };
        final Future<Object> future = executor.submit(task);
        try {
            future.get(300, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            // handle the timeout
            LOG.info("Got time out, nice!");
        } catch (InterruptedException e) {
            fail("Interrupted");
        } catch (ExecutionException e) {
            fail("Some error " + e.getMessage());
            // handle other exceptions
        } finally {
            future.cancel(true);
        }
    }

    /**
     * Try to claim coordinate twice
     */
    @Test
    public void testDoubleClaim() throws CloudnameException, InterruptedException {
        final Coordinate c = Coordinate.parse("2.service.user.cell");
        final CountDownLatch okCounter = new  CountDownLatch(1);
        final CountDownLatch failCounter = new  CountDownLatch(1);

        final CoordinateListener listener = new CoordinateListener() {
            @Override
            public void onCoordinateEvent(Event event, String message) {
                switch (event) {
                    case COORDINATE_OK:
                        okCounter.countDown();
                        break;
                    case NOT_OWNER:
                        failCounter.countDown();
                    default: //Any other Event is unexpected.
                        assert(false);
                        break;
                }
            }
        };
        final Cloudname cn;
        try {
            cn = makeLocalZkCloudname();
        } catch (CloudnameException e) {
            fail("connecting to localhost failed.");
            return;
        }

        try {
            cn.createCoordinate(c);
        } catch (CoordinateExistsException e) {
            fail("should not happen.");
        }
        final ServiceHandle handle1 = cn.claim(c);
        assert(handle1.waitForCoordinateOkSeconds(4));
        handle1.registerCoordinateListener(listener);
        ServiceHandle handle2 = cn.claim(c);
        assertFalse(handle2.waitForCoordinateOkSeconds(1));
        handle2.registerCoordinateListener(listener);
        assert(okCounter.await(4, TimeUnit.SECONDS));
        assert(failCounter.await(2, TimeUnit.SECONDS));
    }


    @Test
    public void testDestroyBasic() throws Exception {
        final Coordinate c = Coordinate.parse("1.service.user.cell");
        final Cloudname cn = makeLocalZkCloudname();
        cn.createCoordinate(c);
        assertTrue(pathExists("/cn/cell/user/service/1/config"));
        cn.destroyCoordinate(c);
        assertFalse(pathExists("/cn/cell/user/service"));
        assertTrue(pathExists("/cn/cell/user"));
    }

    @Test
    public void testDestroyTwoInstances() throws Exception {
        final Coordinate c1 = Coordinate.parse("1.service.user.cell");
        final Coordinate c2 = Coordinate.parse("2.service.user.cell");
        final Cloudname cn = makeLocalZkCloudname();
        cn.createCoordinate(c1);
        cn.createCoordinate(c2);
        assertTrue(pathExists("/cn/cell/user/service/1/config"));
        assertTrue(pathExists("/cn/cell/user/service/2/config"));
        cn.destroyCoordinate(c1);
        assertFalse(pathExists("/cn/cell/user/service/1"));
        assertTrue(pathExists("/cn/cell/user/service/2/config"));
    }

    @Test
    public void testDestroyClaimed() throws Exception {
        final Coordinate c = Coordinate.parse("1.service.user.cell");
        final Cloudname cn = makeLocalZkCloudname();
        cn.createCoordinate(c);
        cn.claim(c);
        try {
            cn.destroyCoordinate(c);
            fail("Expected exception to happen");
        } catch (CoordinateException e) {
        }
    }

    private boolean pathExists(String path) throws Exception {
        return (null != zk.exists(path, false));
    }

    /**
     * Makes a local ZkCloudname instance with the port given by zkPort.
     */
    private ZkCloudname makeLocalZkCloudname() throws CloudnameException {
        return new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();
    }
}
