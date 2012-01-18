package org.cloudname.zk;

import org.cloudname.*;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.*;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;

import org.cloudname.testtools.Net;
import org.cloudname.testtools.zookeeper.EmbeddedZooKeeper;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Unit test for the ZkCloudname class.
 *
 * @author borud
 */
public class ZkCloudnameTest {
    private static Logger log = Logger.getLogger(ZkCloudnameTest.class.getName());

    private EmbeddedZooKeeper ezk;
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

        log.info("EmbeddedZooKeeper rootDir=" + rootDir.getCanonicalPath() + ", port=" + zkport
        );

        // Set up and initialize the embedded ZooKeeper
        ezk = new EmbeddedZooKeeper(rootDir, zkport);
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
                    .connectWithTimeout(1, TimeUnit.NANOSECONDS);
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
        Coordinate c = Coordinate.parse("1.service.user.cell");
        ZkCloudname cn = new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();

        assertFalse(pathExists("/cn/cell/user/service/1"));
        cn.createCoordinate(c);

        // Coordinate should exist, but no status node
        assertTrue(pathExists("/cn/cell/user/service/1"));
        assertTrue(pathExists("/cn/cell/user/service/1/config"));
        assertFalse(pathExists("/cn/cell/user/service/1/status"));

        // Claiming the coordinate creates the status node
        ServiceHandle handle = cn.claim(c);
        assertNotNull(handle);
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

        // Remove one of them
        handle.removeEndpoint("bar");

        ZkStatusAndEndpoints statusAndEndpoints = new ZkStatusAndEndpoints.Builder(
                zk, "/cn/cell/user/service/1/status").build().load();
        assertEquals(null, statusAndEndpoints.getEndpoint("bar"));

        Endpoint endpointFoo = statusAndEndpoints.getEndpoint("foo");
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
     * Try to claim coordinate twice
     */
    @Test
    public void testDoubleClaim() throws CloudnameException, InterruptedException {
        Coordinate c = Coordinate.parse("2.service.user.cell");
        ZkCloudname cn = null;
        try {
            cn = new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();
        } catch (CloudnameException e) {
            fail("connecting to localhost failed.");
        }
        try {
            cn.createCoordinate(c);
            cn.claim(c);
            cn.claim(c);
            fail("Should have thrown exception.");
        } catch (CoordinateException e) {
            // Expected.
        }
    }


    /**
     * Claim non-existing coordinate
     */
    @Test
    public void testCoordinateNotFound() throws CloudnameException, InterruptedException {
        Coordinate c = Coordinate.parse("3.service.user.cell");
        ZkCloudname cn = new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();
        try {
            cn.claim(c);
            fail("Expected coordinate not found thrown.");
        } catch (CoordinateException e) {
        }
    }

    /**
     * A coordinate listener that stores events and calls a latch.
     */
    class UnitTestCoordinateListener implements CoordinateListener {

        final public List<Event> events = new ArrayList<Event>();

        final private CountDownLatch latch;

        UnitTestCoordinateListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onConfigEvent(Event event, String message) {
            events.add(event);
            latch.countDown();
        }
    }

    /**
     * Sets up a claimed coordinate and a connection listener.
     * @param connectedLatch
     * @return
     */
    private UnitTestCoordinateListener setUpListenerEnvironment(CountDownLatch connectedLatch) throws Exception {
        Coordinate c = Coordinate.parse("1.service.user.cell");
        ZkCloudname cn = new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();
        try {
            cn.createCoordinate(c);
        } catch (CoordinateException e) {
            fail(e.toString());
        }
        ServiceHandle handle = cn.claim(c);
        UnitTestCoordinateListener listener = new UnitTestCoordinateListener(connectedLatch);
        handle.registerCoordinateListener(listener);
        return listener;
    }

    @Test
    public void testCoordinateListenerInitialEvent() throws  Exception {
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        UnitTestCoordinateListener listener = setUpListenerEnvironment(connectedLatch);
        assertTrue(connectedLatch.await(2, TimeUnit.SECONDS));
        assertEquals(1, listener.events.size());
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(0));
    }

    @Test
    public void testCoordinateListenerConnectionDies() throws  Exception {

        final CountDownLatch connectedLatch = new CountDownLatch(2);
        UnitTestCoordinateListener listener = setUpListenerEnvironment(connectedLatch);
        log.info("Killing zookeeper");
        ezk.shutdown();
        assertTrue(connectedLatch.await(20, TimeUnit.SECONDS));
        assertEquals(2, listener.events.size());
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(0));
        assertEquals(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE, listener.events.get(1));
    }

    @Test
    public void testCoordinateListenerCoordinateLost() throws  Exception {
        final CountDownLatch connectedLatch = new CountDownLatch(2);
        UnitTestCoordinateListener listener = setUpListenerEnvironment(connectedLatch);
        log.info("Deleting coordinate");
        zk.delete("/cn/cell/user/service/1/status", -1);
        assertTrue(connectedLatch.await(20, TimeUnit.SECONDS));
        assertEquals(2, listener.events.size());
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(0));
        assertEquals(CoordinateListener.Event.COORDINATE_VANISHED, listener.events.get(1));
    }

    @Test
    public void testCoordinateListenerCoordinateCorrupted() throws  Exception {
        final CountDownLatch connectedLatch = new CountDownLatch(2);
        UnitTestCoordinateListener listener = setUpListenerEnvironment(connectedLatch);
        log.info("Corrupting coordinate.");
        String garbage = "sdfgsdfgsfgdsdfgsdfgsdfg";
        byte[] garbageBytes = garbage.getBytes("UTF-16LE");

        zk.setData("/cn/cell/user/service/1/status", garbageBytes, -1);
        assertTrue(connectedLatch.await(20, TimeUnit.SECONDS));
        assertEquals(2, listener.events.size());
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(0));
        assertEquals(CoordinateListener.Event.COORDINATE_CORRUPTED, listener.events.get(1));
    }

    @Test
    public void testCoordinateListenerCoordinateOutOfSync() throws  Exception {
        final CountDownLatch connectedLatch = new CountDownLatch(2);
        UnitTestCoordinateListener listener = setUpListenerEnvironment(connectedLatch);

        log.info("Writing different coordinate.");
        String source = "\"{\\\"state\\\":\\\"STARTING\\\",\\\"message\\\":\\\"Lost hamster.\\\"}\" {}";
        byte[] byteArray = source.getBytes(Util.CHARSET_NAME);

        zk.setData("/cn/cell/user/service/1/status", byteArray, -1);
        assertTrue(connectedLatch.await(20, TimeUnit.SECONDS));
        assertEquals(2, listener.events.size());
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(0));
        assertEquals(CoordinateListener.Event.COORDINATE_OUT_OF_SYNC, listener.events.get(1));
    }

    @Test
    public void testDestroyBasic() throws Exception {
        Coordinate c = Coordinate.parse("1.service.user.cell");
        ZkCloudname cn = new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();
        cn.createCoordinate(c);
        assertTrue(pathExists("/cn/cell/user/service/1/config"));
        cn.destroyCoordinate(c);
        assertFalse(pathExists("/cn/cell/user/service"));
        assertTrue(pathExists("/cn/cell/user"));
    }

    @Test
    public void testDestroyTwoInstances() throws Exception {
        Coordinate c1 = Coordinate.parse("1.service.user.cell");
        Coordinate c2 = Coordinate.parse("2.service.user.cell");
        ZkCloudname cn = new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();
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
        Coordinate c = Coordinate.parse("1.service.user.cell");
        ZkCloudname cn = new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();
        cn.createCoordinate(c);
        ServiceHandle handle = cn.claim(c);
        try {
            cn.destroyCoordinate(c);
            fail("Expected exception to happen");
        } catch (CoordinateException e) {
        }
    }

    private boolean pathExists(String path) throws Exception {
        return (null != zk.exists(path, false));
    }

    private String fetchNodeData(String path) throws Exception {
        return new String(zk.getData(path, null, null), "UTF-8");
    }
}
