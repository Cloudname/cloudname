package org.cloudname.backends.consul;

import org.cloudname.core.BackendManager;
import org.cloudname.core.CloudnameBackend;
import org.cloudname.testtools.backend.CoreBackendTest;
import org.junit.BeforeClass;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assume.assumeThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

/**
 * Test for the Consul backend. Sadly Consul haven't got a nice server we can launch for testing
 * so this test relies on a system property (consul.backend) to run. If the property isn't set it
 * will skip the test completely.
 */
public class ConsulBackendTest extends CoreBackendTest {
    private static final String EP_PROPERTY = "consul.endpoint";
    private static final AtomicReference<CloudnameBackend> backend = new AtomicReference<>(null);

    @BeforeClass
    public static void checkSystemProperty() {
        assumeThat(System.getProperty(EP_PROPERTY), is(notNullValue()));
    }

    @Override
    protected CloudnameBackend getBackend() {
        if (backend.get() == null) {
            backend.compareAndSet(null,
                    BackendManager.getBackend("consul://" + System.getProperty(EP_PROPERTY)));
        }
        return backend.get();
    }

    /**
     * The propagation through Consul isn't as fast as ZooKeeper, at least not for local
     * nodes. The API will occasionally need a second to move everything around, particularly
     * when running inside a Docker container. Give it some extra time to get its act together.
     * (the default is 100ms)
     */
    @Override
    protected int getBackendPropagationTime() {
        return 500;
    }
}
