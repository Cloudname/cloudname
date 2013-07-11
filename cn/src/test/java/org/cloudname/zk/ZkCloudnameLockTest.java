package org.cloudname.zk;

import junit.framework.TestCase;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.cloudname.CloudnameLock;
import org.cloudname.Coordinate;
import org.cloudname.CoordinateException;
import org.cloudname.ServiceHandle;
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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test class for Zookeeper implementation of CloudnameLock.
 *
 * @author acidmoose
 */
public class ZkCloudnameLockTest {

    private static Logger log = Logger.getLogger(ZkCloudnameTest.class.getName());

    private EmbeddedZooKeeper ezk;
    private ZooKeeper zk;
    private int zkport;
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
        File rootDir = temp.newFolder("zklock-test");
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
     * Test that a lock object can not be used twice while a lock is in place.
     */
    @Test
    public void testInternalLockAbuse() throws Exception {
        cn = new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();

        final Coordinate coordinate1 = Coordinate.parse("1.service.user.cell");
        final CloudnameLock.Scope scope = CloudnameLock.Scope.SERVICE;
        final String lockName = "ServiceLockTest1";

        try {
            cn.createCoordinate(coordinate1);
        } catch (CoordinateException e) {
            fail(e.toString());
        }

        final ServiceHandle serviceHandle1 = cn.claim(coordinate1);

        final CloudnameLock lock1 = serviceHandle1.getCloudnameLock(scope, lockName);
        // Attempt to lock
        assertTrue("Unable to lock.", lock1.tryLock());
        // Check that you can not lock twice with the same CloudnameLock object.
        assertFalse("Got lock while a lock is in place.", lock1.tryLock());
        // Release lock.
        lock1.release();
    }

    /**
     * Test a lock without wait on two services on multiple scopes.
     * @throws Exception
     */
    @Test
    public void testLockAndRelease() throws Exception {
        cn = new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();

        final Coordinate coordinate1 = Coordinate.parse("1.service.user.cell");
        final Coordinate coordinate2 = Coordinate.parse("2.service.user.cell");
        final String lockName = "testLockAndRelease";

        try {
            cn.createCoordinate(coordinate1);
            cn.createCoordinate(coordinate2);
        } catch (CoordinateException e) {
            fail(e.toString());
        }

        final ServiceHandle serviceHandle = cn.claim(coordinate1);
        final ServiceHandle serviceHandle2 = cn.claim(coordinate2);

        // Service scope locks
        CloudnameLock.Scope scope = CloudnameLock.Scope.SERVICE;
        CloudnameLock lock1 = serviceHandle.getCloudnameLock(scope, lockName);
        CloudnameLock lock2 = serviceHandle2.getCloudnameLock(scope, lockName);
        // Attempt to lock
        assertTrue("Unable to lock.", lock1.tryLock());
        // Check that you can not obtain another lock on the same scope with the same name.
        assertFalse("Got lock while a lock is not released.", lock2.tryLock());
        // Release lock.
        lock1.release();
        // Attempt to aquire lock number two
        assertTrue("Did not get lock.", lock2.tryLock());
        // And the other way around
        assertFalse("Got lock while a lock is not released.", lock1.tryLock());
        // Clean up
        lock2.release();

        // User scope locks
        scope = CloudnameLock.Scope.USER;
        lock1 = serviceHandle.getCloudnameLock(scope, lockName);
        lock2 = serviceHandle2.getCloudnameLock(scope, lockName);
        assertTrue("Unable to lock.", lock1.tryLock());
        assertFalse("Got lock while a lock is not released.", lock2.tryLock());
        lock1.release();
        assertTrue("Did not get lock.", lock2.tryLock());
        assertFalse("Got lock while a lock is not released.", lock1.tryLock());
        lock2.release();

        // Cell scope locks
        scope = CloudnameLock.Scope.CELL;
        lock1 = serviceHandle.getCloudnameLock(scope, lockName);
        lock2 = serviceHandle2.getCloudnameLock(scope, lockName);
        assertTrue("Unable to lock.", lock1.tryLock());
        assertFalse("Got lock while a lock is not released.", lock2.tryLock());
        lock1.release();
        assertTrue("Did not get lock.", lock2.tryLock());
        assertFalse("Got lock while a lock is not released.", lock1.tryLock());
        lock2.release();
    }

    /**
     * Test a service scope lock with wait on two services where a proper release is performed.
     * @throws Exception
     */
    @Test
    public void testWaitForLockWithProperRelease() throws Exception {
        cn = new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();

        final Coordinate coordinate1 = Coordinate.parse("1.service.user.cell");
        final Coordinate coordinate2 = Coordinate.parse("2.service.user.cell");
        final CloudnameLock.Scope scope = CloudnameLock.Scope.SERVICE;
        final String lockName = "testWaitForLockWithProperRelease";

        try {
            cn.createCoordinate(coordinate1);
            cn.createCoordinate(coordinate2);
        } catch (CoordinateException e) {
            fail(e.toString());
        }

        final ServiceHandle serviceHandle1 = cn.claim(coordinate1);
        final ServiceHandle serviceHandle2 = cn.claim(coordinate2);

        final CloudnameLock lock1 = serviceHandle1.getCloudnameLock(scope, lockName);
        final CloudnameLock lock2 = serviceHandle2.getCloudnameLock(scope, lockName);

        final CountDownLatch latch = new CountDownLatch(1);

        // Attempt to lock
        assertTrue("Unable to lock.", lock1.tryLock());

        Thread thread = new Thread() {
            @Override
            public void run() {
                if (lock2.tryLock(5000))
                    latch.countDown();
            }
        };
        thread.start();

        // Release lock.
        lock1.release();

        assertTrue(latch.await(6000, TimeUnit.MILLISECONDS));
    }

    /**
     * Test a service scope lock with wait on several services where a proper release is performed.
     * @throws Exception
     */
    @Test
    public void testComplexWaitForLock() throws Exception {
        cn = new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();

        final CloudnameLock.Scope scope = CloudnameLock.Scope.SERVICE;
        final String lockName = "testComplexWaitForLock";
        final int NUM_JOBS = 100;

        // Have some time to simulate work on each thread to assure no services performs work at the same time.
        final int WORKLOAD_MS = 5;

        // Timeouts well above needed, but low enough to not hog time on build servers.
        final int LOCK_TIMEOUT_MS = 60000; // 1 min
        final int TEST_TIMEOUT_MS = 120000; // 2 min

        final List<CloudnameLock> lockList = new ArrayList<CloudnameLock>();

        for (int i = 0; i <NUM_JOBS; i++) {
            final Coordinate coordinate = Coordinate.parse(i + ".service.user.cell");
            try {
                cn.createCoordinate(coordinate);
            } catch (CoordinateException e) {
                fail(e.toString());
            }
            final ServiceHandle serviceHandle = cn.claim(coordinate);
            lockList.add(serviceHandle.getCloudnameLock(scope, lockName));
        }

        final CountDownLatch released = new CountDownLatch(NUM_JOBS - 1);
        final CountDownLatch threadReadyLatch = new CountDownLatch(NUM_JOBS - 1);

        // Lock the first service lock
        lockList.get(0).tryLock();

        final Work work = new Work(WORKLOAD_MS);

        for (int i = 1; i < NUM_JOBS; i++) {
            final int num = i;
            final Thread thread = new Thread() {
                @Override
                public void run() {
                    threadReadyLatch.countDown();
                    lockList.get(num).tryLock(LOCK_TIMEOUT_MS);
                    work.doWork();
                    lockList.get(num).release();
                    released.countDown();
                }
            };
            thread.start();
        }

        assertTrue(
            "Threads were not ready. Remaining threads that did not start: " + threadReadyLatch.getCount() + ".",
            threadReadyLatch.await(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS));

        // Release first lock and wait for the mountain to fall...
        lockList.get(0).release();

        assertTrue(
            "Not all jobs completed. Remaining jobs: " + released.getCount() + ".",
            released.await(TEST_TIMEOUT_MS, TimeUnit.MILLISECONDS));

    }

    /**
     * Check that a lock with a partial name match to another lock behaves correctly.
     * @throws Exception
     */
    @Test
    public void testNamePrefix () throws Exception {
        cn = new ZkCloudname.Builder().setConnectString("localhost:" + zkport).build().connect();
        final Coordinate coordinate = Coordinate.parse("1.service.user.cell");
        cn.createCoordinate(coordinate);
        final ServiceHandle serviceHandle = cn.claim(coordinate);
        final CloudnameLock.Scope scope = CloudnameLock.Scope.SERVICE;
        CloudnameLock lock1 = serviceHandle.getCloudnameLock(scope, "aaa");
        CloudnameLock lock2 = serviceHandle.getCloudnameLock(scope, "aaabbb");

        assertTrue("Did not get lock.", lock2.tryLock());
        assertTrue("Did not get lock.", lock1.tryLock());

        lock1.release();
        lock2.release();
    }

    /**
     * Class simulates a shared resource that can not be used by more than one at a time.
     */
    private class Work {
        private boolean busy = false;
        private final long simulatedWorkTime;

        public Work(long simulatedWorkTime) {
            this.simulatedWorkTime = simulatedWorkTime;
        }

        public void doWork() {
            if (busy)
                fail("Work is already being done.");
            busy = true;
            try {
                Thread.sleep(simulatedWorkTime);
            } catch (InterruptedException e) {
                fail("InterruptedException while simulating work.");
            }
            busy = false;
        }
    }
}
