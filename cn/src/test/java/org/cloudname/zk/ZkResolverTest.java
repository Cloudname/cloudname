package org.cloudname.zk;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.cloudname.*;
import org.cloudname.testtools.Net;
import org.cloudname.testtools.zookeeper.EmbeddedZooKeeper;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;


/**
 * This class contains the unit tests for the ZkResolver class.
 *
 * TODO(borud): add tests for when the input is a coordinate.
 *
 * @author borud
 */
public class ZkResolverTest {
    private EmbeddedZooKeeper ezk;
    private ZooKeeper zk;
    private int zkport;
    private ZkCloudname cn;
    private Coordinate coordinateRunning;
    private Coordinate coordinateDraining;
    @Rule public TemporaryFolder temp = new TemporaryFolder();
    private ServiceHandle handleDraining;

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
        coordinateRunning = Coordinate.parse("1.service.user.cell");
        cn = new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();
        cn.createCoordinate(coordinateRunning);
        ServiceHandle handleRunning = cn.claim(coordinateRunning);

        handleRunning.putEndpoint(new Endpoint(coordinateRunning, "foo", "localhost", 1234, "http", "data")).
                waitForCompletionMillis(5999);
        handleRunning.putEndpoint(new Endpoint(coordinateRunning, "bar", "localhost", 1235, "http", null))
                .waitForCompletionMillis(5999);
        ServiceStatus statusRunning = new ServiceStatus(ServiceState.RUNNING, "Running message");
        handleRunning.setStatus(statusRunning).waitForCompletionMillis(5999);;

        coordinateDraining = Coordinate.parse("0.service.user.cell");
        cn.createCoordinate(coordinateDraining);
        handleDraining = cn.claim(coordinateDraining);
        handleDraining.putEndpoint(new Endpoint(coordinateDraining, "foo", "localhost", 5555, "http", "data"))
                .waitForCompletionMillis(5999);;
        handleDraining.putEndpoint(new Endpoint(coordinateDraining, "bar", "localhost", 5556, "http", null))
                .waitForCompletionMillis(5999);

        ServiceStatus statusDraining = new ServiceStatus(ServiceState.DRAINING, "Draining message");
        handleDraining.setStatus(statusDraining).waitForCompletionMillis(5999);;
    }

    public void undrain() {
        ServiceStatus statusDraining = new ServiceStatus(ServiceState.RUNNING, "alive");
        StorageFuture future = handleDraining.setStatus(statusDraining);
        future.waitForCompletionMillis(5999);
    }
    public void drain() {
        ServiceStatus statusDraining = new ServiceStatus(ServiceState.DRAINING, "dead");
        StorageFuture future = handleDraining.setStatus(statusDraining);
        future.waitForCompletionMillis(5999);
    }

    public void changeEndpoint() {
        handleDraining.removeEndpoint("foo").waitForCompletionMillis(4999);
        handleDraining.putEndpoint(new Endpoint(coordinateDraining, "foo", "localhost", 4, "http", "data"))
                .waitForCompletionMillis(5000);
    }

    @After
    public void tearDown() throws Exception {
        zk.close();
    }
    // Valid endpoints.
    public static final String[] validEndpointPatterns = new String[] {
        "http.1.service.user.cell",
        "foo-bar.3245.service.user.cell",
        "foo_bar.3245.service.user.cell",
        "foo_bar.3245.service.user.cell",
    };

    // Valid strategy.
    public static final String[] validStrategyPatterns = new String[] {
        "any.service.user.cell",
        "all.service.user.cell",
        "somestrategy.service.user.cell",
    };

    // Valid endpoint strategy.
    public static final String[] validEndpointStrategyPatterns = new String[] {
        "http.any.service.user.cell",
        "thrift.all.service.user.cell",
        "some-endpoint.somestrategy.service.user.cell",
    };

    @Test
    public void testStatus() throws Exception {
        ServiceStatus status = cn.getStatus(coordinateRunning);
        assertEquals(ServiceState.RUNNING, status.getState());

        assertEquals("Running message", status.getMessage());
    }
    
    
    @Test
    public void testBasicSyncResolving() throws Exception {
        Resolver resolver = cn.getResolver();
        List<Endpoint> endpoints = resolver.resolve("foo.1.service.user.cell");
        assertEquals(1, endpoints.size());
        assertEquals("foo", endpoints.get(0).getName());
        assertEquals("localhost", endpoints.get(0).getHost());
        assertEquals("1.service.user.cell", endpoints.get(0).getCoordinate().toString());
        assertEquals("data", endpoints.get(0).getEndpointData());
        assertEquals("http", endpoints.get(0).getProtocol());
    }

    @Test
    public void testBasicAsyncResolving() throws Exception {
        Resolver resolver = cn.getResolver();
        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(2);
        final CountDownLatch latch3 = new CountDownLatch(1);
        final CountDownLatch latchModified = new CountDownLatch(1);

        final List<Endpoint> endpointListNew = new ArrayList<Endpoint>();
        final List<Endpoint> endpointListModified = new ArrayList<Endpoint>();
        final List<Endpoint> endpointListRemoved = new ArrayList<Endpoint>();
        resolver.addResolverListener("foo.all.service.user.cell", new Resolver.ResolverListener() {

            @Override
            public void endpointEvent(Event event, String endpointId, Endpoint endpoint) {
                switch (event) {

                    case NEW_ENDPOINT:
                        System.err.println("Got new endpoint.");
                        endpointListNew.add(endpoint);
                        latch1.countDown();
                        latch2.countDown();
                        break;
                    case MODIFIED_ENDPOINT:
                        System.err.println("Modified endpoint.");
                        endpointListModified.add(endpoint);
                        latchModified.countDown();
                        break;
                    case REMOVED_ENDPOINT:
                        System.err.println("Removed endpoint.");
                        endpointListRemoved.add(endpoint);
                        latch3.countDown();
                        break;
                }
            }
        });
        assertTrue(latch1.await(5000, TimeUnit.MILLISECONDS));
        assertEquals(1, endpointListNew.size());
        assertEquals("foo", endpointListNew.get(0).getName());
        assertEquals("1.service.user.cell", endpointListNew.get(0).getCoordinate().toString());
        undrain();

        assertTrue(latch2.await(5000, TimeUnit.MILLISECONDS));
        assertEquals(2, endpointListNew.size());

        assertEquals("foo", endpointListNew.get(1).getName());
        assertEquals("0.service.user.cell", endpointListNew.get(1).getCoordinate().toString());

        changeEndpoint();

        assertTrue(latchModified.await(2500, TimeUnit.MILLISECONDS));
        assertEquals("foo", endpointListModified.get(0).getName());
        assertEquals("0.service.user.cell", endpointListModified.get(0).getCoordinate().toString());
        assertEquals(4, endpointListModified.get(0).getPort());

        drain();

        assertTrue(latch3.await(5000, TimeUnit.MILLISECONDS));

        assertEquals(1, endpointListRemoved.size());

        assertEquals("foo", endpointListRemoved.get(0).getName());
        assertEquals("0.service.user.cell", endpointListRemoved.get(0).getCoordinate().toString());
    }
    
    @Test
    public void testAnyResolving() throws Exception {
        Resolver resolver = cn.getResolver();
        List<Endpoint> endpoints = resolver.resolve("foo.any.service.user.cell");
        assertEquals(1, endpoints.size());
        assertEquals("foo", endpoints.get(0).getName());
        assertEquals("localhost", endpoints.get(0).getHost());
        assertEquals("1.service.user.cell", endpoints.get(0).getCoordinate().toString());
    }

    @Test
    public void testAllResolving() throws Exception {
        Resolver resolver = cn.getResolver();
        List<Endpoint> endpoints = resolver.resolve("all.service.user.cell");
        assertEquals(2, endpoints.size());
        assertEquals("foo", endpoints.get(0).getName());
        assertEquals("bar", endpoints.get(1).getName());
    }

    @Test
    public void testEndpointPatterns() throws Exception {
        // Test input that should match
        for (String s : validEndpointPatterns) {
            assertTrue("Didn't match '" + s + "'",
                       ZkResolver.endpointPattern.matcher(s).matches());
        }

        // Test input that should not match
        for (String s : validStrategyPatterns) {
            assertFalse("Matched '" + s + "'",
                        ZkResolver.endpointPattern.matcher(s).matches());
        }

        // Test input that should not match
        for (String s : validEndpointStrategyPatterns) {
            assertFalse("Matched '" + s + "'",
                        ZkResolver.endpointPattern.matcher(s).matches());
        }
    }

    @Test
    public void testStrategyPatterns() throws Exception {
        // Test input that should match
        for (String s : validStrategyPatterns) {
            assertTrue("Didn't match '" + s + "'",
                       ZkResolver.strategyPattern.matcher(s).matches());
        }

        // Test input that should not match
        for (String s : validEndpointPatterns) {
            assertFalse("Matched '" + s + "'",
                        ZkResolver.strategyPattern.matcher(s).matches());
        }
        // Test input that should not match
        for (String s : validEndpointStrategyPatterns) {
            assertFalse("Matched '" + s + "'",
                        ZkResolver.endpointPattern.matcher(s).matches());
        }
    }

    @Test
    public void testEndpointStrategyPatterns() throws Exception {
        // Test input that should match
        for (String s : validEndpointStrategyPatterns) {
            assertTrue("Didn't match '" + s + "'",
                       ZkResolver.endpointStrategyPattern.matcher(s).matches());
        }

        // Test input that should not match
        for (String s : validStrategyPatterns) {
            assertFalse("Matched '" + s + "'",
                        ZkResolver.endpointStrategyPattern.matcher(s).matches());
        }


        // Test input that should not match
        for (String s : validEndpointPatterns) {
            assertFalse("Matched '" + s + "'",
                        ZkResolver.endpointStrategyPattern.matcher(s).matches());
        }
    }
}