package org.cloudname.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.cloudname.*;
import org.cloudname.testtools.Net;
import org.cloudname.testtools.network.PortForwarder;
import org.cloudname.testtools.zookeeper.EmbeddedZooKeeper;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Logger;

import static org.junit.Assert.*;

/**
 * Integration tests for testing ZkCloudname.
 * Contains mostly heavy tests containing sleep calls not fit as a unit test.
 */
public class ZkCloudnameIntegrationTest {
    private static Logger log = Logger.getLogger(ZkCloudnameTest.class.getName());

    private EmbeddedZooKeeper ezk;
    private ZooKeeper zk;
    private int zkport;
    private PortForwarder forwarder;
    private int forwarderPort;
    private ZkCloudname cn = null;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

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

        log.info("EmbeddedZooKeeper rootDir=" + rootDir.getCanonicalPath() + ", port=" + zkport);

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
     * A coordinate listener that stores events and calls a latch.
     */
    class TestCoordinateListener implements CoordinateListener {
        final public List<Event> events = new CopyOnWriteArrayList<Event>();

        final private CountDownLatch latch1, latch2;

        TestCoordinateListener(CountDownLatch latch1, CountDownLatch latch2) {
            this.latch1 = latch1;
            this.latch2 = latch2;
        }

        @Override
        public void onCoordinateEvent(Event event, String message) {
            System.err.println("Got unit test even " + event.toString() + " " + message);
            events.add(event);
            latch1.countDown();
            latch2.countDown();
        }
    }

    private TestCoordinateListener setUpListenerEnvironment(
            CountDownLatch connectedLatch1, CountDownLatch connectedLatch2) throws Exception {
        forwarderPort = Net.getFreePort();
        forwarder = new PortForwarder(forwarderPort, "127.0.0.1", zkport);
        final Coordinate c = Coordinate.parse("1.service.user.cell");

        cn = makeLocalZkCloudname(forwarderPort);
        try {
            cn.createCoordinate(c);
        } catch (CoordinateException e) {
            fail(e.toString());
        }
        final TestCoordinateListener listener = new TestCoordinateListener(connectedLatch1, connectedLatch2);
        ServiceHandle serviceHandle = cn.claim(c);
        serviceHandle.registerCoordinateListener(listener);

        return listener;
    }

    @Test
    public void testCoordinateListenerInitialEvent() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(2);
        final CountDownLatch connectedLatch2 = new CountDownLatch(2);
        final TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(15, TimeUnit.SECONDS));
        assertEquals(2, listener.events.size());
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(1));
        forwarder.terminate();
    }

    @Test
    public void testCoordinateListenerConnectionDies() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final CountDownLatch connectedLatch2 = new CountDownLatch(2);
        final TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        log.info("Killing zookeeper");
        ezk.shutdown();
        forwarder.terminate();
        assertTrue(connectedLatch2.await(20, TimeUnit.SECONDS));
        final int size = listener.events.size();
        assertTrue(size > 1);
        log.info("status " + listener.events.toString());
        assertEquals(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE, listener.events.get(size - 1));
    }

    @Test
    public void testCoordinateListenerCoordinateCorrupted() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(2);
        final CountDownLatch connectedLatch2 = new CountDownLatch(3);
        final TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        log.info("Corrupting coordinate.");
        byte[] garbageBytes = "sdfgsdfgsfgdsdfgsdfgsdfg".getBytes("UTF-16LE");

        zk.setData("/cn/cell/user/service/1/status", garbageBytes, -1);
        assertTrue(connectedLatch2.await(20, TimeUnit.SECONDS));
        assertEquals(3, listener.events.size());
        assertEquals(CoordinateListener.Event.COORDINATE_OUT_OF_SYNC, listener.events.get(2));
        forwarder.terminate();
    }

    @Test
    public void testCoordinateListenerCoordinateOutOfSync() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(2);
        final CountDownLatch connectedLatch2 = new CountDownLatch(4);
        final TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));

        log.info("Writing different coordinate.");
        String source = "\"{\\\"state\\\":\\\"STARTING\\\",\\\"message\\\":\\\"Lost hamster.\\\"}\" {}";
        byte[] byteArray = source.getBytes(Util.CHARSET_NAME);

        zk.setData("/cn/cell/user/service/1/status", byteArray, -1);
        log.info("Done writing different coordinate.");
        assertTrue(connectedLatch2.await(20, TimeUnit.SECONDS));

        assertEquals(CoordinateListener.Event.NOT_OWNER, listener.events.get(3));
        forwarder.terminate();
    }

    @Test
    public void testCoordinateListenerCoordinateLost() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final CountDownLatch connectedLatch2 = new CountDownLatch(3);
        TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        log.info("Deleting coordinate");
        forwarder.terminate();
        zk.delete("/cn/cell/user/service/1/status", -1);
        zk.delete("/cn/cell/user/service/1/config", -1);
        zk.delete("/cn/cell/user/service/1", -1);
        forwarder = new PortForwarder(forwarderPort, "127.0.0.1", zkport);
        assertTrue(connectedLatch2.await(20, TimeUnit.SECONDS));
        int i = 0;
        while (listener.events.get(listener.events.size() - 1 ) != CoordinateListener.Event.NOT_OWNER) {
            Thread.sleep(30);
            ++i;
            if (i > 100) {
                fail("Did not get COORDINATE_VANISHED");
            }
        }
        forwarder.terminate();
    }

    @Test
    public void testCoordinateListenerStolenCoordinate() throws Exception {

        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final CountDownLatch connectedLatch2 = new CountDownLatch(2);
        TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        log.info("Killing zookeeper");
        assertTrue(zk.getState() == ZooKeeper.States.CONNECTED);

        log.info("Killing connection");
        forwarder.terminate();

        zk.delete("/cn/cell/user/service/1/status", -1);
        Util.mkdir(zk, "/cn/cell/user/service/1/status" , ZooDefs.Ids.OPEN_ACL_UNSAFE);

        forwarder = new PortForwarder(forwarderPort, "127.0.0.1", zkport);

        assertTrue(connectedLatch2.await(6, TimeUnit.SECONDS));

        int i = 0;
        int q = -1;
        while (true) {
            if (q != listener.events.size()) {
                q = listener.events.size();
            }
            if (listener.events.get(listener.events.size() - 1 ) == CoordinateListener.Event.NOT_OWNER) {
                break;
            }

            Thread.sleep(10);
            ++i;
            if (i > 1000) {
                fail("Did not get NOT_OWNER");
            }
        }

        //cn2.close();
        // We use the same path for the new ezk, so it reads up the old state, and hence the coordinate is ok.
//        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(2));
        forwarder.terminate();
    }


    @Test
    public void testCoordinateListenerConnectionDiesReconnect() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(2);
        final CountDownLatch connectedLatch2 = new CountDownLatch(4);
        TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));


        log.info("Killing connection");
        forwarder.terminate();
        log.info("Recreating connection" + forwarderPort + "->" + zkport);

        forwarder = new PortForwarder(forwarderPort, "127.0.0.1", zkport);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(listener.events.size() -1 ));
    }

    /**
     * In This test the ZK server thinks the client is connected, but the client wants to reconnect due to a disconnect.
     * This test might be flaky since it has timing with sleeps. If it becomes a problem we disable the test.
     * It works on my computer and is useful for debugging the reconnect functionality.
     */
    @Test
    public void testCoordinateListenerConnectionDiesReconnectAfterTimeoutClient() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(2);
        final CountDownLatch connectedLatch2 = new CountDownLatch(6);
        TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        assertEquals(CoordinateListener.Event.COORDINATE_OK,
                listener.events.get(listener.events.size() -1 ));
        log.info("Killing connection");
        forwarder.terminate();

        log.info("Connection down.");

        Thread.sleep(3400);
        log.info("Recreating connection soon" + forwarderPort + "->" + zkport);

        assertEquals(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                listener.events.get(listener.events.size() -1 ));
        forwarder = new PortForwarder(forwarderPort, "127.0.0.1", zkport);
        assertTrue(connectedLatch2.await(20, TimeUnit.SECONDS));


        for (int c = 0; c < 100; c++) {
            if (CoordinateListener.Event.COORDINATE_OK == listener.events.get(listener.events.size() -1 ))  {
                break;
            }
            Thread.sleep(100);
        }
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(listener.events.size() -1 ));

        forwarder.terminate();
    }

    @Test
    public void testCoordinateListenerConnectionDiesReconnectAfterTimeout() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(2);
        final CountDownLatch connectedLatch2 = new CountDownLatch(6);
        TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        assertEquals(CoordinateListener.Event.COORDINATE_OK,
                listener.events.get(listener.events.size() -1 ));
        log.info("Killing connection");
        forwarder.terminate();

        log.info("Connection down.");

        Thread.sleep(900);
        log.info("Recreating connection soon" + forwarderPort + "->" + zkport);
        Thread.sleep(200);
        assertEquals(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                listener.events.get(listener.events.size() -1 ));
        forwarder = new PortForwarder(forwarderPort, "127.0.0.1", zkport);
        assertTrue(connectedLatch2.await(20, TimeUnit.SECONDS));


        for (int c = 0; c < 100; c++) {
            if (CoordinateListener.Event.COORDINATE_OK == listener.events.get(listener.events.size() -1 ))  {
                break;
            }
            Thread.sleep(300);
        }
        Thread.sleep(450);
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(listener.events.size() -1 ));

        forwarder.terminate();
    }


    /**
     * Tests the behavior of Zookeeper upon a restart. ZK should clean up old coordinates.
     * @throws Exception
     */
    @Test
    public void testZookeeperRestarts() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final CountDownLatch connectedLatch2 = new CountDownLatch(3);
        TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1, connectedLatch2);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        log.info("Killing zookeeper");
        forwarder.terminate();

        ezk.shutdown();
        ezk.del();
        ezk.init();
        Thread.sleep(2000);
        forwarder = new PortForwarder(forwarderPort, "127.0.0.1", zkport);

        int timeoutSecs = 30;
        while (--timeoutSecs > 0) {
            Thread.sleep(1000);
        }
        Coordinate c = Coordinate.parse("1.service.user.cell");
        cn.createCoordinate(c);

        Thread.sleep(9000);
        assertEquals(listener.events.get(listener.events.size() - 1), CoordinateListener.Event.COORDINATE_OK);
    }

    /**
     * Tests that one process claims a coordinate, then another process tries to claim the same coordinate.
     * The first coordinate looses connection to ZooKeeper and the other process gets the coordinate.
     * @throws Exception
     */
    @Test
    public void testFastHardRestart() throws Exception {
        final Coordinate c = Coordinate.parse("1.service.user.cell");
        final CountDownLatch claimLatch1 = new CountDownLatch(1);
        forwarderPort = Net.getFreePort();
        forwarder = new PortForwarder(forwarderPort, "127.0.0.1", zkport);
        Cloudname cn1 = new ZkCloudname.Builder().setConnectString("localhost:" + forwarderPort).build().connect();
        cn1.createCoordinate(c);

        ServiceHandle handle1 = cn1.claim(c);
        handle1.registerCoordinateListener(new CoordinateListener() {

            @Override
            public void onCoordinateEvent(Event event, String message) {
                if (event == Event.COORDINATE_OK) {
                    claimLatch1.countDown();
                }
            }
        });
        assertTrue(claimLatch1.await(5, TimeUnit.SECONDS));

        Cloudname cn2 = new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();

        ServiceHandle handle2 = cn2.claim(c);

        forwarder.terminate();

        assertTrue(handle2.waitForCoordinateOkSeconds(20));

        ServiceStatus status = new ServiceStatus(ServiceState.RUNNING, "updated status");
        handle2.setStatus(status);

        Cloudname cn3 = new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();
        ServiceStatus statusRetrieved = cn3.getStatus(c);
        assertEquals("updated status", statusRetrieved.getMessage());
    }

    /**
     * Makes a local ZkCloudname instance with the port given by zkPort.
     * Then it connects to ZK.
     */
    private ZkCloudname makeLocalZkCloudname(int port) throws CloudnameException {
        return new ZkCloudname.Builder().setConnectString("localhost:" + port).build().connect();
    }
}
