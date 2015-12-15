package org.cloudname.service;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class ServiceDataTest {
    @Test
    public void testCreation() {
        final Endpoint ep1 = new Endpoint("foo", "bar", 1);
        final Endpoint ep2 = new Endpoint("bar", "baz", 1);

        final ServiceData data = new ServiceData(Arrays.asList(ep1, ep2));
        assertThat(data.getEndpoint("foo"), is(equalTo(ep1)));
        assertThat(data.getEndpoint("bar"), is(equalTo(ep2)));
        assertThat(data.getEndpoint("baz"), is(nullValue()));
    }

    @Test
    public void testAddRemoveEndpoint() {
        final ServiceData data = new ServiceData(new ArrayList<Endpoint>());
        assertThat(data.getEndpoint("a"), is(nullValue()));
        assertThat(data.getEndpoint("b"), is(nullValue()));

        final Endpoint ep1 = new Endpoint("a", "localhost", 80);
        final Endpoint ep1a = new Endpoint("a", "localhost", 80);
        // Endpoint can only be added once
        assertThat(data.addEndpoint(ep1), is(true));
        assertThat(data.addEndpoint(ep1), is(false));
        // Endpoints must be unique
        assertThat(data.addEndpoint(ep1a), is(false));

        // Another endpoint can be added
        final Endpoint ep2 = new Endpoint("b", "localhost", 80);
        final Endpoint ep2a = new Endpoint("b", "localhost", 80);
        assertThat(data.addEndpoint(ep2), is(true));
        // But the same rules applies
        assertThat(data.addEndpoint(ep2), is(false));
        assertThat(data.addEndpoint(ep2a), is(false));

        // Data now contains both endpoints
        assertThat(data.getEndpoint("a"), is(equalTo(ep1)));
        assertThat(data.getEndpoint("b"), is(equalTo(ep2)));

        assertThat(data.removeEndpoint(ep1), is(true));
        assertThat(data.removeEndpoint(ep1a), is(false));

        // ...ditto for next endpoint
        assertThat(data.removeEndpoint(ep2), is(true));
        assertThat(data.removeEndpoint(ep2), is(false));

        // The endpoints with identical names can be added
        assertThat(data.addEndpoint(ep1a), is(true));
        assertThat(data.addEndpoint(ep2a), is(true));
    }

    @Test
    public void testConversionToFromJson() {
        final Endpoint endpointA = new Endpoint("foo", "bar", 80);
        final Endpoint endpointB = new Endpoint("baz", "bar", 81);
        final ServiceData dataA = new ServiceData(
                Arrays.asList(endpointA, endpointB));

        final String jsonString = dataA.toJsonString();

        final ServiceData dataB = ServiceData.fromJsonString(jsonString);

        assertThat(dataB.getEndpoint("foo"), is(endpointA));
        assertThat(dataB.getEndpoint("baz"), is(endpointB));
    }

    @Test
    public void uniqueNamesAreRequired() {
        final Endpoint endpointA = new Endpoint("foo", "bar", 80);
        final Endpoint endpointB = new Endpoint("foo", "baz", 82);
        final Endpoint endpointC = new Endpoint("foo", "localhost", 80);
        final Endpoint endpointD = new Endpoint("foobar", "localhost", 80);

        final ServiceData serviceData = new ServiceData(new ArrayList<Endpoint>());
        assertThat(serviceData.addEndpoint(endpointA), is(true));
        assertThat(serviceData.addEndpoint(endpointB), is(false));
        assertThat(serviceData.addEndpoint(endpointC), is(false));
        assertThat(serviceData.addEndpoint(endpointD), is(true));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidJson1() {
        final String nullStr = null;
        ServiceData.fromJsonString(nullStr);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidJson2() {
        ServiceData.fromJsonString("");
    }

    @Test (expected = org.json.JSONException.class)
    public void testInvalidJson3() {
        ServiceData.fromJsonString("}{");
    }

    @Test (expected = org.json.JSONException.class)
    public void testInvalidJson4() {
        ServiceData.fromJsonString("{ \"foo\": 12 }");
    }

    @Test (expected = IllegalArgumentException.class)
    public void addNullEndpoint() {
        final ServiceData data = new ServiceData();
        data.addEndpoint(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void removeNullEndpoint() {
        final ServiceData data = new ServiceData();
        data.removeEndpoint(null);
    }
}
