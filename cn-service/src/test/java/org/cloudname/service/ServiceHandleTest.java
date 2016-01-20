package org.cloudname.service;

import org.cloudname.core.CloudnamePath;
import org.cloudname.core.LeaseHandle;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ServiceHandleTest {

    @Test
    public void testCreation() {
        final InstanceCoordinate instanceCoordinate
                = InstanceCoordinate.parse("instance.service.tag.region");
        final ServiceData serviceData = new ServiceData(new ArrayList<Endpoint>());
        final LeaseHandle handle = new LeaseHandle() {
            @Override
            public boolean writeData(String data) {
                return true;
            }

            @Override
            public CloudnamePath getLeasePath() {
                return instanceCoordinate.toCloudnamePath();
            }

            @Override
            public void close() throws IOException {
                // nothing
            }
        };

        final ServiceHandle serviceHandle
                = new ServiceHandle(instanceCoordinate, serviceData, handle);

        final Endpoint ep1 = new Endpoint("foo", "bar", 80);
        assertThat(serviceHandle.registerEndpoint(ep1), is(true));
        assertThat(serviceHandle.registerEndpoint(ep1), is(false));

        assertThat(serviceHandle.removeEndpoint(ep1), is(true));
        assertThat(serviceHandle.removeEndpoint(ep1), is(false));

        serviceHandle.close();
    }

    @Test
    public void testFailingHandle() {
        final InstanceCoordinate instanceCoordinate
                = InstanceCoordinate.parse("instance.service.tag.region");
        final Endpoint ep1 = new Endpoint("foo", "bar", 80);

        final ServiceData serviceData = new ServiceData(Arrays.asList(ep1));
        final LeaseHandle handle = new LeaseHandle() {
            @Override
            public boolean writeData(String data) {
                return false;
            }

            @Override
            public CloudnamePath getLeasePath() {
                return instanceCoordinate.toCloudnamePath();
            }

            @Override
            public void close() throws IOException {
                throw new IOException("I broke");
            }
        };

        final ServiceHandle serviceHandle
                = new ServiceHandle(instanceCoordinate, serviceData, handle);

        final Endpoint ep2 = new Endpoint("bar", "baz", 81);
        assertThat(serviceHandle.registerEndpoint(ep2), is(false));

        assertThat(serviceHandle.removeEndpoint(ep1), is(false));
        assertThat(serviceHandle.removeEndpoint(ep2), is(false));

        serviceHandle.close();
    }

    @Test (expected = IllegalArgumentException.class)
    public void testWithNullParameters1() {
        new ServiceHandle(null, null, null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testWithNullParameters2() {
        new ServiceHandle(InstanceCoordinate.parse("a.b.c.d"), null, null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testWithNullParameters3() {
        new ServiceHandle(
                InstanceCoordinate.parse("a.b.c.d"),
                new ServiceData(new ArrayList<Endpoint>()),
                null);
    }
}
