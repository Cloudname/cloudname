package org.cloudname.backends.zookeeper;

import org.apache.curator.test.TestingCluster;
import org.cloudname.core.CloudnameBackend;
import org.cloudname.testtools.backend.CoreBackendTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Test the ZooKeeper backend.
 */
public class ZooKeeperBackendTest extends CoreBackendTest {
    private static TestingCluster testCluster;
    private AtomicReference<ZooKeeperBackend> backend = new AtomicReference<>(null);

    @BeforeClass
    public static void setUp() throws Exception {
        testCluster = new TestingCluster(3);
        testCluster.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testCluster.stop();
    }

    protected CloudnameBackend getBackend() {
        if (backend.get() == null) {
            backend.compareAndSet(null, new ZooKeeperBackend(testCluster.getConnectString()));
        }
        return backend.get();

    }
}
