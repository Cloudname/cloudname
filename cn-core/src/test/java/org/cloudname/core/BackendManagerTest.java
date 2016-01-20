package org.cloudname.core;

import org.junit.Test;

import java.util.function.Function;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

/**
 * Test the BackendManager class with a few mock backends
 */
public class BackendManagerTest {
    @Test (expected = IllegalArgumentException.class)
    public void invalidDriverUrl() {
        BackendManager.getBackend(":/some-random-string");
    }

    @Test
    public void nullDriverUrl() {
        assertThat(BackendManager.getBackend(null), is(nullValue()));
    }

    /**
     * Create a mock backend
     */
    private CloudnameBackend createBackend() {
        return new CloudnameBackend() {
            @Override
            public LeaseHandle createLease(LeaseType type, CloudnamePath path, String data) {
                return null;
            }

            @Override
            public boolean writeLeaseData(CloudnamePath path, String data) {
                return false;
            }

            @Override
            public String readLeaseData(CloudnamePath path) {
                return null;
            }

            @Override
            public boolean removeLease(CloudnamePath path) {
                return false;
            }

            @Override
            public void addLeaseCollectionListener(
                    CloudnamePath pathToObserve, LeaseListener listener) {

            }

            @Override
            public void addLeaseListener(CloudnamePath pathToObserve, LeaseListener listener) {

            }

            @Override
            public void removeLeaseListener(LeaseListener listener) {

            }

            @Override
            public void close() throws Exception {

            }
        };
    }

    @Test
    public void registerUnregisterDrivers() {
        final CloudnameBackend aBackend = createBackend();
        final CloudnameBackend bBackend = createBackend();

        BackendManager.register("foo", (v) -> { assertThat(v, is("fooString")); return aBackend; } );
        BackendManager.register("bar", (v) -> { assertThat(v, is("barString")); return bBackend; } );

        assertThat(BackendManager.getBackend("foo://fooString"), is(aBackend));
        assertThat(BackendManager.getBackend("bar://barString"), is(bBackend));

        BackendManager.deregister("foo");
        assertThat(BackendManager.getBackend("foo://string"), is(nullValue()));

        BackendManager.deregister("bar");
        assertThat(BackendManager.getBackend("bar://string"), is(nullValue()));
    }
}
