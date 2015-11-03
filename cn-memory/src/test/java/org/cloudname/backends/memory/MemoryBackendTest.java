package org.cloudname.backends.memory;

import org.cloudname.core.CloudnameBackend;
import org.cloudname.testtools.backend.CoreBackendTest;

/**
 * Test the memory backend. Since the memory backend is the reference implementation this test
 * shouldn't fail. Ever.
 */
public class MemoryBackendTest extends CoreBackendTest {
    private static final CloudnameBackend BACKEND = new MemoryBackend();

    @Override
    protected CloudnameBackend getBackend() {
        return BACKEND;
    }

}
