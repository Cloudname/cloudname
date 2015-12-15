package org.cloudname.backends.memory;

import org.cloudname.core.BackendManager;
import org.cloudname.core.CloudnameBackend;
import org.cloudname.testtools.backend.CoreBackendTest;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

/**
 * Test the memory backend. Since the memory backend is the reference implementation this test
 * shouldn't fail. Ever.
 */
public class MemoryBackendTest extends CoreBackendTest {
    private static final CloudnameBackend BACKEND = BackendManager.getBackend("memory://");

    @Override
    protected CloudnameBackend getBackend() {
        assertThat("Expected backend to be registered", BACKEND, is(notNullValue()));
        return BACKEND;
    }

}
