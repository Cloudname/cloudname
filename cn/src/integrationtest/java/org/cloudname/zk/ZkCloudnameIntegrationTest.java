package org.cloudname.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.cloudname.Cloudname;
import org.cloudname.CloudnameException;
import org.cloudname.Coordinate;
import org.cloudname.CoordinateException;
import org.cloudname.CoordinateExistsException;
import org.cloudname.CoordinateListener;
import org.cloudname.ServiceHandle;
import org.cloudname.ServiceState;
import org.cloudname.ServiceStatus;
import org.cloudname.testtools.Net;
import org.cloudname.testtools.network.PortForwarder;
import org.cloudname.testtools.zookeeper.EmbeddedZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.Assert.*;

/**
 * Integration tests for testing ZkCloudname.
 * Contains mostly heavy tests containing sleep calls not fit as a unit test.
 */
public class ZkCloudnameIntegrationTest {
    private static final Logger LOG = Logger.getLogger(ZkCloudnameIntegrationTest.class.getName());

    private EmbeddedZooKeeper ezk;
    private ZooKeeper zk;
    private int zkport;
    private PortForwarder forwarder = null;
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

        LOG.info("EmbeddedZooKeeper rootDir=" + rootDir.getCanonicalPath() + ", port=" + zkport);

        // Set up and initialize the embedded ZooKeeper
        ezk = new EmbeddedZooKeeper(rootDir, zkport);
        ezk.init();

        // Set up a zookeeper client that we can use for inspection
        final CountDownLatch connectedLatch = new CountDownLatch(1);

        zk = new ZooKeeper("localhost:" + zkport, 1000, new Watcher() {
            @Override
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
        if (forwarder != null) {
            forwarder.close();
        }
        ezk.shutdown();
    }

    /**
     * A coordinate listener that stores events and calls a latch.
     */
    class TestCoordinateListener implements CoordinateListener {
        private final List<Event> events = new CopyOnWriteArrayList<Event>();

        private final Set<CountDownLatch> listenerLatches;

        private final List<Event> waitForEvent = new ArrayList<Event>();
        private final Object eventMonitor = new Object();
        private final List<CountDownLatch> waitForLatch = new ArrayList<CountDownLatch>();

        public boolean failOnWrongEvent = false;
        private CountDownLatch latestLatch = null;

        void waitForExpected() throws InterruptedException {
            final CountDownLatch latch;
            synchronized (eventMonitor) {
                if (waitForEvent.size() > 0) {
                    LOG.info("Waiting for event " + waitForEvent.get(waitForEvent.size() - 1));
                    latch = latestLatch;
                } else {
                    return;
                }
            }
            assert(latch.await(25, TimeUnit.SECONDS));
            LOG.info("Event happened.");
        }

        public TestCoordinateListener(final Set<CountDownLatch> listenerLatches) {
            this.listenerLatches = listenerLatches;
        }

        public void expectEvent(final Event event) {
            LOG.info("Expecting event " + event.name());
            synchronized (eventMonitor) {
                waitForEvent.add(event);
                latestLatch = new CountDownLatch(1);
                waitForLatch.add(latestLatch);
            }
        }

        @Override
        public void onCoordinateEvent(Event event, String message) {
            LOG.info("I got event ..." + event.name() + " " + message);
            synchronized (eventMonitor) {
                if (waitForEvent.size() > 0) {
                    LOG.info("Waiting for event " + waitForEvent.get(0));
                }   else {
                    LOG.info("not expecting any specific events");
                }
                events.add(event);
                for (CountDownLatch countDownLatch :listenerLatches) {
                    countDownLatch.countDown();
                }
                if (waitForEvent.size() > 0 && waitForEvent.get(0) == event) {
                    waitForLatch.remove(0).countDown();
                    waitForEvent.remove(0);
                } else {
                    assertFalse(failOnWrongEvent);
                }
            }
        }
    }

    private TestCoordinateListener setUpListenerEnvironment(
            final CountDownLatch latch) throws Exception {
        Set<CountDownLatch> latches = new HashSet<CountDownLatch>();
        latches.add(latch);
        return setUpListenerEnvironment(latches);
    }

    private TestCoordinateListener setUpListenerEnvironment(
            final Set<CountDownLatch> listenerLatches) throws Exception {
        forwarderPort = Net.getFreePort();
        forwarder = new PortForwarder(forwarderPort, "127.0.0.1", zkport);
        final Coordinate c = Coordinate.parse("1.service.user.cell");

        cn = makeLocalZkCloudname(forwarderPort);
        try {
            cn.createCoordinate(c);
        } catch (CoordinateException e) {
            fail(e.toString());
        }
        final TestCoordinateListener listener = new TestCoordinateListener(listenerLatches);
        ServiceHandle serviceHandle = cn.claim(c);
        assert(serviceHandle.waitForCoordinateOkSeconds(3 /* secs */));
        serviceHandle.registerCoordinateListener(listener);

        return listener;
    }

    @Test
    public void testCoordinateListenerInitialEvent() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1);
        assertTrue(connectedLatch1.await(15, TimeUnit.SECONDS));
        assertEquals(1, listener.events.size());
        assertEquals(CoordinateListener.Event.COORDINATE_OK, listener.events.get(0));
    }

    @Test
    public void testCoordinateListenerConnectionDies() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1);
         assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));

        listener.expectEvent(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE);
        forwarder.close();
        forwarder = null;
        listener.waitForExpected();
    }

    @Test
    public void testCoordinateListenerCoordinateCorrupted() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));

        listener.expectEvent(CoordinateListener.Event.NOT_OWNER);

        byte[] garbageBytes = "sdfgsdfgsfgdsdfgsdfgsdfg".getBytes("UTF-16LE");

        zk.setData("/cn/cell/user/service/1/status", garbageBytes, -1);
        listener.waitForExpected();
    }

    @Test
    public void testCoordinateListenerCoordinateOutOfSync() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));

        listener.expectEvent(CoordinateListener.Event.NOT_OWNER);

        String source = "\"{\\\"state\\\":\\\"STARTING\\\",\\\"message\\\":\\\"Lost hamster.\\\"}\" {}";
        byte[] byteArray = source.getBytes(Util.CHARSET_NAME);

        zk.setData("/cn/cell/user/service/1/status", byteArray, -1);

        listener.waitForExpected();
    }

    @Test
    public void testCoordinateListenerCoordinateLost() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1);
        listener.expectEvent(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE);
        listener.expectEvent(CoordinateListener.Event.NOT_OWNER);

        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        LOG.info("Deleting coordinate");
        forwarder.pause();
        zk.delete("/cn/cell/user/service/1/status", -1);
        zk.delete("/cn/cell/user/service/1/config", -1);
        zk.delete("/cn/cell/user/service/1", -1);
        forwarder.unpause();

        listener.waitForExpected();

    }

    @Test
    public void testCoordinateListenerStolenCoordinate() throws Exception {

        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        LOG.info("Killing zookeeper");
        assertTrue(zk.getState() == ZooKeeper.States.CONNECTED);

        LOG.info("Killing connection");
        forwarder.pause();

        zk.delete("/cn/cell/user/service/1/status", -1);
        Util.mkdir(zk, "/cn/cell/user/service/1/status" , ZooDefs.Ids.OPEN_ACL_UNSAFE);

        forwarder.unpause();

        listener.expectEvent(CoordinateListener.Event.NOT_OWNER);
        listener.waitForExpected();
    }


    @Test
    public void testCoordinateListenerConnectionDiesReconnect() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));

        listener.expectEvent(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE);

        forwarder.pause();
        listener.waitForExpected();

        listener.expectEvent(CoordinateListener.Event.COORDINATE_OK);
        forwarder.unpause();
        listener.waitForExpected();
    }

    /**
     * In this test the ZK server thinks the client is connected, but the client wants to reconnect
     * due to a disconnect. To trig this condition the connection needs to be down for
     * a specific time. This test does not fail even if it does not manage to create this
     * state. It will write the result to the log. The test is useful for development and
     * should not fail.
     */
    @Test
    public void testCoordinateListenerConnectionDiesReconnectAfterTimeoutClient()
            throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        assertEquals(CoordinateListener.Event.COORDINATE_OK,
                listener.events.get(listener.events.size() -1 ));

        listener.expectEvent(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE);
        LOG.info("Killing connection");
        forwarder.pause();

        LOG.info("Connection down.");
        listener.waitForExpected();

        // Client sees problem, server not.
        listener.expectEvent(CoordinateListener.Event.COORDINATE_OK);

        // 3400 is a magic number for getting zookeeper and local client in a specific state.
        Thread.sleep(2400);
        LOG.info("Recreating connection soon" + forwarderPort + "->" + zkport);


        forwarder.unpause();
        listener.waitForExpected();   // COORDINATE_OK

        // If the previous event is NOT_OWNER, the wanted situation was created by the test.
        if (listener.events.get(listener.events.size() - 2) ==
                CoordinateListener.Event.NOT_OWNER) {
            LOG.info("Manage to trig event inn ZooKeeper, true positive.");
        } else {
            LOG.info("Did NOT manage to trig event in ZooKeeper. This depends on timing, so " +
                    "ignoring this problem");
        }
    }

    @Test
    public void testCoordinateListenerConnectionDiesReconnectAfterTimeout() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));
        assertEquals(CoordinateListener.Event.COORDINATE_OK,
                listener.events.get(listener.events.size() -1 ));

        listener.expectEvent(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE);

        forwarder.close();
        forwarder = null;
        listener.waitForExpected();
        // We do not want NOT OWNER event from ZooKeeper. Therefore this long time out.
        LOG.info("Going into sleep, waiting for zookeeper to loose node");
        Thread.sleep(10000);

        listener.expectEvent(CoordinateListener.Event.COORDINATE_OK);
        forwarder = new PortForwarder(forwarderPort, "127.0.0.1", zkport);

        // We need to re-instantiate the forwarder, or zookeeper thinks
        // the connection is good and will not kill the ephemeral node.
        // This is probably because we keep the server socket against zookeeper open
        // in pause mode.

        listener.waitForExpected();
    }


    /**
     * Tests the behavior of Zookeeper upon a restart. ZK should clean up old coordinates.
     * @throws Exception
     */
    @Test
    public void testZookeeperRestarts() throws  Exception {
        final CountDownLatch connectedLatch1 = new CountDownLatch(1);
        final TestCoordinateListener listener = setUpListenerEnvironment(connectedLatch1);
        assertTrue(connectedLatch1.await(20, TimeUnit.SECONDS));


        listener.expectEvent(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE);
        forwarder.pause();
        listener.waitForExpected();

        ezk.shutdown();
        ezk.del();
        ezk.init();

        listener.expectEvent(CoordinateListener.Event.NOT_OWNER);

        forwarder.unpause();
        listener.waitForExpected();

        createCoordinateWithRetries();

        listener.expectEvent(CoordinateListener.Event.COORDINATE_OK);
        listener.waitForExpected();
    }

    private void createCoordinateWithRetries() throws CoordinateExistsException,
            InterruptedException, CloudnameException {
        Coordinate c = Coordinate.parse("1.service.user.cell");
        int retries = 10;
        for (;;) {
            try {
                cn.createCoordinate(c);
                break;
            } catch (CloudnameException e) {
                /*
                 * CloudnameException indicates that the connection with
                 * ZooKeeper isn't back up yet. Retry a few times.
                 */
                if (retries-- > 0) {
                    LOG.info("Failed to create coordinate: " + e
                            + ", retrying in 1 second");
                    Thread.sleep(1000);
                } else {
                    throw e;
                }
            }
        }
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
        final Cloudname cn1 = new ZkCloudname.Builder().setConnectString(
                "localhost:" + forwarderPort).build().connect();
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

        final Cloudname cn2 = new ZkCloudname.Builder().setConnectString(
                "localhost:" + zkport).build().connect();

        ServiceHandle handle2 = cn2.claim(c);

        forwarder.close();
        forwarder = null;

        assertTrue(handle2.waitForCoordinateOkSeconds(20));

        ServiceStatus status = new ServiceStatus(ServiceState.RUNNING, "updated status");
        handle2.setStatus(status);

        final Cloudname cn3 = new ZkCloudname.Builder().setConnectString("localhost:" + zkport)
                .build().connect();
        ServiceStatus statusRetrieved = cn3.getStatus(c);
        assertEquals("updated status", statusRetrieved.getMessage());

        cn1.close();
        cn2.close();
        cn3.close();
    }

    /**
     * Makes a local ZkCloudname instance with the port given by zkPort.
     * Then it connects to ZK.
     */
    private ZkCloudname makeLocalZkCloudname(int port) throws CloudnameException {
        return new ZkCloudname.Builder().setConnectString("localhost:" + port).build().connect();
    }
}
