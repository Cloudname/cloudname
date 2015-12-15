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
            public LeaseHandle createTemporaryLease(CloudnamePath path, String data) {
                return null;
            }

            @Override
            public boolean writeTemporaryLeaseData(CloudnamePath path, String data) {
                return false;
            }

            @Override
            public String readTemporaryLeaseData(CloudnamePath path) {
                return null;
            }

            @Override
            public void addTemporaryLeaseListener(CloudnamePath pathToWatch, LeaseListener listener) {

            }

            @Override
            public void removeTemporaryLeaseListener(LeaseListener listener) {

            }

            @Override
            public boolean createPermanantLease(CloudnamePath path, String data) {
                return false;
            }

            @Override
            public boolean removePermanentLease(CloudnamePath path) {
                return false;
            }

            @Override
            public boolean writePermanentLeaseData(CloudnamePath path, String data) {
                return false;
            }

            @Override
            public String readPermanentLeaseData(CloudnamePath path) {
                return null;
            }

            @Override
            public void addPermanentLeaseListener(CloudnamePath pathToObserver, LeaseListener listener) {

            }

            @Override
            public void removePermanentLeaseListener(LeaseListener listener) {

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
