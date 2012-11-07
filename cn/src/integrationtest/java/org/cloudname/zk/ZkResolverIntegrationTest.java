package org.cloudname.zk;

import org.apache.zookeeper.*;
import org.cloudname.*;
import org.cloudname.testtools.Net;
import org.cloudname.testtools.zookeeper.EmbeddedZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Integration tests for the ZkResolver class.
 * This test class contains tests dependent on timing or
 * tests depending on other modules, or both.
 */
public class ZkResolverIntegrationTest {
    private ZooKeeper zk;
    private Cloudname cn;
    private Coordinate coordinateRunning;
    private Coordinate coordinateDraining;
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    private ServiceHandle handleDraining;


    /**
     * Set up an embedded ZooKeeper instance backed by a temporary
     * directory.  The setup procedure also allocates a port that is
     * free for the ZooKeeper server so that you should be able to run
     * multiple instances of this test.
     * TODO: This exact same method is in ZkResolverTest.
     * Should there even be an integration test for ZkResolver? All tests only depend on ZK.
     * Maybe merge this class with ZkResolverTest yet again?
     */
    @Before
    public void setup() throws Exception {
        // Speed up tests waiting for this event to happen.
        DynamicExpression.TIME_BETWEEN_NODE_SCANNING_MS = 200;

        File rootDir = temp.newFolder("zk-test");
        final int zkport = Net.getFreePort();

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
        coordinateRunning = Coordinate.parse("1.service.user.cell");
        cn = new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();
        cn.createCoordinate(coordinateRunning);
        ServiceHandle handleRunning = cn.claim(coordinateRunning);
        assertTrue(handleRunning.waitForCoordinateOkSeconds(30));

        handleRunning.putEndpoint(new Endpoint(coordinateRunning, "foo", "localhost", 1234, "http", "data"));
        handleRunning.putEndpoint(new Endpoint(coordinateRunning, "bar", "localhost", 1235, "http", null));
        ServiceStatus statusRunning = new ServiceStatus(ServiceState.RUNNING, "Running message");
        handleRunning.setStatus(statusRunning);

        coordinateDraining = Coordinate.parse("0.service.user.cell");
        cn.createCoordinate(coordinateDraining);
        handleDraining = cn.claim(coordinateDraining);
        assertTrue(handleDraining.waitForCoordinateOkSeconds(10));
        handleDraining.putEndpoint(new Endpoint(coordinateDraining, "foo", "localhost", 5555, "http", "data"));
        handleDraining.putEndpoint(new Endpoint(coordinateDraining, "bar", "localhost", 5556, "http", null));

        ServiceStatus statusDraining = new ServiceStatus(ServiceState.DRAINING, "Draining message");
        handleDraining.setStatus(statusDraining);
    }

    @After
    public void tearDown() throws Exception {
        zk.close();
    }


    public void undrain() throws CoordinateMissingException, CloudnameException {
        ServiceStatus statusDraining = new ServiceStatus(ServiceState.RUNNING, "alive");
        handleDraining.setStatus(statusDraining);
    }

    public void drain() throws CoordinateMissingException, CloudnameException {
        ServiceStatus statusDraining = new ServiceStatus(ServiceState.DRAINING, "dead");
        handleDraining.setStatus(statusDraining);
    }

    public void changeEndpoint() throws CoordinateMissingException, CloudnameException {
        handleDraining.removeEndpoint("foo");
        handleDraining.putEndpoint(new Endpoint(coordinateDraining, "foo", "localhost", 4, "http", "data"));
    }

    @Test
    public void testStatus() throws Exception {
        ServiceStatus status = cn.getStatus(coordinateRunning);
        assertEquals(ServiceState.RUNNING, status.getState());
        assertEquals("Running message", status.getMessage());
    }

    /**
     * Test an unclaimed coordinate and a path that is not complete.
     * Number of endpoints should not increase when inputting bad data.
     * @throws Exception
     */
    @Test
    public void testGetCoordinateDataAllNoClaimedCoordinate() throws Exception {
        // Create unclaimned coordinate.
        Coordinate coordinateNoStatus = Coordinate.parse("4.service.user.cell");
        cn.createCoordinate(coordinateNoStatus);

        // Throw in a incomplete path.
        zk.create("/cn/foo",  new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        Resolver resolver = cn.getResolver();

        Resolver.CoordinateDataFilter filter = new Resolver.CoordinateDataFilter();
        Set<Endpoint> endpoints = resolver.getEndpoints(filter);
        assertEquals(4, endpoints.size());
    }

    @Test
    public void testBasicAsyncResolving() throws Exception {
        Resolver resolver = cn.getResolver();

        final List<Endpoint> endpointListNew = new ArrayList<Endpoint>();
        final List<Endpoint> endpointListRemoved = new ArrayList<Endpoint>();

        // This class is needed since the abstract resolver listener class can only access final variables.
        class LatchWrapper {
            public CountDownLatch latch;
        }
        final LatchWrapper latchWrapper = new LatchWrapper();

        latchWrapper.latch = new CountDownLatch(1);

        resolver.addResolverListener("foo.all.service.user.cell", new Resolver.ResolverListener() {

            @Override
            public void endpointEvent(Event event, Endpoint endpoint) {
                switch (event) {

                    case NEW_ENDPOINT:
                        endpointListNew.add(endpoint);
                        latchWrapper.latch.countDown();
                        break;
                    case REMOVED_ENDPOINT:
                        endpointListRemoved.add(endpoint);
                        latchWrapper.latch.countDown();
                        break;
                }
            }
        });
        assertTrue(latchWrapper.latch.await(5000, TimeUnit.MILLISECONDS));
        assertEquals(1, endpointListNew.size());
        assertEquals("foo", endpointListNew.get(0).getName());
        assertEquals("1.service.user.cell", endpointListNew.get(0).getCoordinate().toString());
        endpointListNew.clear();

        latchWrapper.latch = new CountDownLatch(1);

        undrain();

        assertTrue(latchWrapper.latch.await(5000, TimeUnit.MILLISECONDS));
        assertEquals(1, endpointListNew.size());

        assertEquals("foo", endpointListNew.get(0).getName());
        assertEquals("0.service.user.cell", endpointListNew.get(0).getCoordinate().toString());

        latchWrapper.latch = new CountDownLatch(2);
        endpointListNew.clear();

        changeEndpoint();

        assertTrue(latchWrapper.latch.await(5000, TimeUnit.MILLISECONDS));

        assertEquals(1, endpointListRemoved.size());

        assertEquals("0.service.user.cell", endpointListRemoved.get(0).getCoordinate().toString());
        assertEquals("foo", endpointListRemoved.get(0).getName());
        assertEquals("foo", endpointListNew.get(0).getName());
        assertEquals("0.service.user.cell", endpointListNew.get(0).getCoordinate().toString());
        assertEquals(4, endpointListNew.get(0).getPort());

        endpointListNew.clear();
        endpointListRemoved.clear();
        latchWrapper.latch = new CountDownLatch(1);

        drain();

        assertTrue(latchWrapper.latch.await(5000, TimeUnit.MILLISECONDS));

        assertEquals(1, endpointListRemoved.size());

        assertEquals("0.service.user.cell", endpointListRemoved.get(0).getCoordinate().toString());
        assertEquals("foo", endpointListRemoved.get(0).getName());
    }

    @Test
    public void testBasicAsyncResolvingAnyStrategy() throws Exception {
        Resolver resolver = cn.getResolver();

        final List<Endpoint> endpointListNew = new ArrayList<Endpoint>();

        // This class is needed since the abstract resolver listener class can only access final variables.
        class LatchWrapper {
            public CountDownLatch latch;
        }
        final LatchWrapper latchWrapper = new LatchWrapper();

        latchWrapper.latch = new CountDownLatch(1);

        resolver.addResolverListener("foo.any.service.user.cell", new Resolver.ResolverListener() {

            @Override
            public void endpointEvent(Event event, Endpoint endpoint) {
                switch (event) {

                    case NEW_ENDPOINT:
                        endpointListNew.add(endpoint);
                        latchWrapper.latch.countDown();
                        break;
                    case REMOVED_ENDPOINT:
                        latchWrapper.latch.countDown();
                        break;
                }
            }
        });
        assertTrue(latchWrapper.latch.await(2000, TimeUnit.MILLISECONDS));
        assertEquals(1, endpointListNew.size());
        assertEquals("foo", endpointListNew.get(0).getName());
        assertEquals("1.service.user.cell", endpointListNew.get(0).getCoordinate().toString());
        endpointListNew.clear();

        latchWrapper.latch = new CountDownLatch(1);

        undrain();

        assertFalse(latchWrapper.latch.await(2000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testStopAsyncResolving() throws Exception {
        Resolver resolver = cn.getResolver();

        final List<Endpoint> endpointListNew = new ArrayList<Endpoint>();

        // This class is needed since the abstract resolver listener class can only access final variables.
        class LatchWrapper {
            public CountDownLatch latch;
        }
        final LatchWrapper latchWrapper = new LatchWrapper();

        latchWrapper.latch = new CountDownLatch(1);


        Resolver.ResolverListener resolverListener = new Resolver.ResolverListener() {
            @Override
            public void endpointEvent(Event event, Endpoint endpoint) {
                switch (event) {

                    case NEW_ENDPOINT:
                        endpointListNew.add(endpoint);
                        latchWrapper.latch.countDown();
                        break;
                    case REMOVED_ENDPOINT:
                        latchWrapper.latch.countDown();
                        break;
                }
            }
        };
        resolver.addResolverListener("foo.all.service.user.cell", resolverListener);
        assertTrue(latchWrapper.latch.await(2000, TimeUnit.MILLISECONDS));
        assertEquals(1, endpointListNew.size());
        assertEquals("foo", endpointListNew.get(0).getName());
        assertEquals("1.service.user.cell", endpointListNew.get(0).getCoordinate().toString());
        endpointListNew.clear();

        latchWrapper.latch = new CountDownLatch(1);

        resolver.removeResolverListener(resolverListener);

        undrain();

        assertFalse(latchWrapper.latch.await(100, TimeUnit.MILLISECONDS));

        try {
            resolver.removeResolverListener(resolverListener);
        } catch (IllegalArgumentException e) {
            // This is expected.
            return;
        }
        fail("Did not throw an exception on deleting a non existing listener.");
    }
}
