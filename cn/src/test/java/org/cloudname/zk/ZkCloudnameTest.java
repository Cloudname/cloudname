package org.cloudname.zk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import org.cloudname.Cloudname;
import org.cloudname.CloudnameException;
import org.cloudname.ConfigListener;
import org.cloudname.Coordinate;
import org.cloudname.CoordinateException;
import org.cloudname.CoordinateExistsException;
import org.cloudname.CoordinateListener;
import org.cloudname.Endpoint;
import org.cloudname.ServiceHandle;
import org.cloudname.ServiceState;
import org.cloudname.ServiceStatus;
import org.cloudname.testtools.Net;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test for the ZkCloudname class.
 *
 * @author borud, dybdahl
 */
public class ZkCloudnameTest {
    private static final Logger LOG = Logger.getLogger(ZkCloudnameTest.class.getName());

    private final EmbeddedZooKeeperWithClient embeddedZooKeeperWithClient =
            new EmbeddedZooKeeperWithClient();

    @Rule public TemporaryFolder temp = new TemporaryFolder();

    /**
     * Set up an embedded ZooKeeper instance backed by a temporary
     * directory.  The setup procedure also allocates a port that is
     * free for the ZooKeeper server so that you should be able to run
     * multiple instances of this test.
     */
    @Before
    public void setup() throws Exception {
        embeddedZooKeeperWithClient.setup(temp);
    }

    @After
    public void tearDown() throws Exception {
        embeddedZooKeeperWithClient.close();
    }

    /**
     * Tests that the time-out mechanism on connecting to ZooKeeper works.
     */
    @Test
    public void testTimeout() throws IOException, InterruptedException {
        final int deadPort = Net.getFreePort();
        try {
            new ZkCloudname.Builder().setConnectString("localhost:" + deadPort).build()
                    .connectWithTimeout(1000, TimeUnit.NANOSECONDS);
            fail("Expected time-out exception.");
        } catch (final CloudnameException e) {
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
            public void onCoordinateEvent(final Event event, final String message) {
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
            public void onConfigEvent(final Event event, final String data) {
                buffer.append(data);
                configLatch1.countDown();
                configLatch2.countDown();
            }
        });
        assertTrue(configLatch1.await(5, TimeUnit.SECONDS));
        assertEquals(buffer.toString(), "");
        embeddedZooKeeperWithClient.getZk().setData("/cn/cell/user/service/1/config",
                "hello".getBytes(), -1);
        assertTrue(configLatch2.await(5, TimeUnit.SECONDS));
        assertEquals(buffer.toString(), "hello");

        assertTrue(pathExists("/cn/cell/user/service/1/status"));

        final List<String> nodes = new ArrayList<String>();
        cn.listRecursively(nodes);
        assertEquals(2, nodes.size());
        assertEquals(nodes.get(0), "/cn/cell/user/service/1/config");
        assertEquals(nodes.get(1), "/cn/cell/user/service/1/status");

        // Try to set the status to something else
        final String msg = "Hamster getting quite eager now";
        handle.setStatus(new ServiceStatus(ServiceState.STARTING,msg));
        final ServiceStatus status = cn.getStatus(c);
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
        final Endpoint endpointFoo = endpointList.get(0);

        final String fooData = endpointFoo.getName();
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
            @Override
            public Object call() throws InterruptedException {
                return cn.claim(c);
            }
        };
        final Future<Object> future = executor.submit(task);
        try {
            future.get(300, TimeUnit.MILLISECONDS);
        } catch (final TimeoutException ex) {
            // handle the timeout
            LOG.info("Got time out, nice!");
        } catch (final InterruptedException e) {
            fail("Interrupted");
        } catch (final ExecutionException e) {
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
            public void onCoordinateEvent(final Event event, final String message) {
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
        } catch (final CloudnameException e) {
            fail("connecting to localhost failed.");
            return;
        }

        try {
            cn.createCoordinate(c);
        } catch (final CoordinateExistsException e) {
            fail("should not happen.");
        }
        final ServiceHandle handle1 = cn.claim(c);
        assert(handle1.waitForCoordinateOkSeconds(4));
        handle1.registerCoordinateListener(listener);
        final ServiceHandle handle2 = cn.claim(c);
        assertFalse(handle2.waitForCoordinateOkSeconds(2));
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
        final ServiceHandle handle = cn.claim(c);
        handle.waitForCoordinateOkSeconds(1);
        try {
            cn.destroyCoordinate(c);
            fail("Expected exception to happen");
        } catch (final CoordinateException e) {
        }
    }

    private boolean pathExists(final String path) throws Exception {
        return (null != embeddedZooKeeperWithClient.getZk().exists(path, false));
    }

    /**
     * Makes a local ZkCloudname instance with the port given by zkPort.
     */
    private ZkCloudname makeLocalZkCloudname() throws CloudnameException {
        return new ZkCloudname.Builder()
                .setConnectString("localhost:" + embeddedZooKeeperWithClient.getZkPort()).build()
                .connect();
    }
}
