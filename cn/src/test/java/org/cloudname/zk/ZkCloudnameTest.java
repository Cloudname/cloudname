package org.cloudname.zk;

import org.cloudname.*;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.cloudname.testtools.network.PortForwarder;
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
    private PortForwarder forwarder;
    private int forwarderPort;

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

        System.out.println("ZooKeeper port is " + zkport);
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

        final private CountDownLatch latch1, latch2;
        
        UnitTestCoordinateListener(CountDownLatch latch1, CountDownLatch latch2) {
            this.latch1 = latch1;
            this.latch2 = latch2;
        }

        @Override
        public void onConfigEvent(Event event, String message) {
            System.err.println("Got unit test even " + event.toString() + " " + message);
            events.add(event);
            latch1.countDown();
            latch2.countDown();
        }
    }

    /**
     * Sets up a claimed coordinate and a connection listener.
     * @param connectedLatch
     * @return
     */
    private UnitTestCoordinateListener setUpListenerEnvironment(
            CountDownLatch connectedLatch1, CountDownLatch connectedLatch2) throws Exception {
        forwarderPort = Net.getFreePort();
        forwarder = new PortForwarder(forwarderPort, "127.0.0.1", zkport);
        Coordinate c = Coordinate.parse("1.service.user.cell");
        ZkCloudname cn = new ZkCloudname.Builder().setConnectString("localhost:" + forwarderPort).build().connect();
        try {
            cn.createCoordinate(c);
        } catch (CoordinateException e) {
            fail(e.toString());
        }
        ServiceHandle handle = cn.claim(c);
        UnitTestCoordinateListener listener = new UnitTestCoordinateListener(connectedLatch1, connectedLatch2);
        handle.registerCoordinateListener(listener);
        return listener;
    }

    @Test
    public void testCoordinateListenerInitialEvent() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final CountDownLatch connectedLatch2 = new CountDownLatch(1);
        UnitTestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(2, TimeUnit.SECONDS));
        assertEquals(1, listener.events.size());
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(0));
        forwarder.terminate();
    }

    @Test
    public void testCoordinateListenerConnectionDies() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final CountDownLatch connectedLatch2 = new CountDownLatch(2);
        UnitTestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        log.info("Killing zookeeper");
        ezk.shutdown();
        assertTrue(connectedLatch2.await(20, TimeUnit.SECONDS));
        assertEquals(2, listener.events.size());
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(0));
        assertEquals(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE, listener.events.get(1));
        forwarder.terminate();
    }

    @Test
    public void testCoordinateListenerCoordinateLost() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final CountDownLatch connectedLatch2 = new CountDownLatch(2);
        UnitTestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        log.info("Deleting coordinate");
        zk.delete("/cn/cell/user/service/1/status", -1);
        assertTrue(connectedLatch2.await(20, TimeUnit.SECONDS));
        assertEquals(2, listener.events.size());
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(0));
        assertEquals(CoordinateListener.Event.COORDINATE_VANISHED, listener.events.get(1));
        forwarder.terminate();
    }

    @Test
    public void testCoordinateListenerCoordinateCorrupted() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final CountDownLatch connectedLatch2 = new CountDownLatch(2);
        UnitTestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        log.info("Corrupting coordinate.");
        String garbage = "sdfgsdfgsfgdsdfgsdfgsdfg";
        byte[] garbageBytes = garbage.getBytes("UTF-16LE");

        zk.setData("/cn/cell/user/service/1/status", garbageBytes, -1);
        assertTrue(connectedLatch2.await(20, TimeUnit.SECONDS));
        assertEquals(2, listener.events.size());
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(0));
        assertEquals(CoordinateListener.Event.COORDINATE_OUT_OF_SYNC, listener.events.get(1));
        forwarder.terminate();
    }

    @Test
    public void testCoordinateListenerCoordinateOutOfSync() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final CountDownLatch connectedLatch2 = new CountDownLatch(2);
        UnitTestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));

        log.info("Writing different coordinate.");
        String source = "\"{\\\"state\\\":\\\"STARTING\\\",\\\"message\\\":\\\"Lost hamster.\\\"}\" {}";
        byte[] byteArray = source.getBytes(Util.CHARSET_NAME);

        zk.setData("/cn/cell/user/service/1/status", byteArray, -1);
        assertTrue(connectedLatch2.await(20, TimeUnit.SECONDS));
        assertEquals(2, listener.events.size());
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(0));
        assertEquals(CoordinateListener.Event.COORDINATE_OUT_OF_SYNC, listener.events.get(1));
        forwarder.terminate();
    }

    @Test
    public void testCoordinateListenerZooKeeperBlackout() throws Exception {

        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final CountDownLatch connectedLatch2 = new CountDownLatch(3);
        UnitTestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        log.info("Killing zookeeper");
        assertTrue(zk.getState() == ZooKeeper.States.CONNECTED);
        ezk.shutdown();
        while (zk.getState() != ZooKeeper.States.CONNECTED) {
            System.out.println("waiting for correct state: " + zk.getState().toString());
            Thread.sleep(30);
        }
        ezk.init();
        assertTrue(connectedLatch2.await(6, TimeUnit.SECONDS));
        assertEquals(3, listener.events.size());
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(0));
        assertEquals(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE, listener.events.get(1));

        // We use the same path for the new ezk, so it reads up the old state, and hence the coordinate is ok.
//        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(2));
        forwarder.terminate();
    }


    @Test
    public void testCoordinateListenerConnectionDiesReconnect() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final CountDownLatch connectedLatch2 = new CountDownLatch(3);
        UnitTestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));


        log.info("Killing connection");
        forwarder.terminate();
        log.info("Recreating connection" + forwarderPort + "->" + zkport);

        forwarder = new PortForwarder(forwarderPort, "127.0.0.1", zkport);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(listener.events.size() -1 ));
    }

    @Test
    public void testCoordinateListenerConnectionDiesReconnectAfterTimeout() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final CountDownLatch connectedLatch2 = new CountDownLatch(5);
        UnitTestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        assertEquals(CoordinateListener.Event.COORDINATE_OK,
                listener.events.get(listener.events.size() -1 ));
        log.info("Killing connection");
        forwarder.terminate();

        log.info("Connection down.");

        Thread.sleep(6000);
        log.info("Recreating connection soon" + forwarderPort + "->" + zkport);
        Thread.sleep(1000);
        assertEquals(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE,
                listener.events.get(listener.events.size() -1 ));
        forwarder = new PortForwarder(forwarderPort, "127.0.0.1", zkport);
        assertTrue(connectedLatch2.await(20, TimeUnit.SECONDS));


        for (int c = 0; c < 100; c++) {
            if (CoordinateListener.Event.COORDINATE_OK == listener.events.get(listener.events.size() -1 ))  {
                break;
            }
            Thread.sleep(300);
        }
        Thread.sleep(500);
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(listener.events.size() -1 ));

        forwarder.terminate();
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
