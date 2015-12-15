package org.cloudname.service;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.hamcrest.CoreMatchers.is;

/**
 * Test the Endpoint class. Relatively straightforward; test creation and that
 * fields are set correctly, test conversion to and from JSON, test the equals()
 * implementation and test assertions in constructor.
 */
public class EndpointTest {
    @Test
    public void testCreation() {
        final Endpoint endpoint = new Endpoint("foo", "localhost", 80);
        assertThat(endpoint.getName(), is("foo"));
        assertThat(endpoint.getHost(), is("localhost"));
        assertThat(endpoint.getPort(), is(80));
    }

    @Test
    public void testJsonConversion() {
        final Endpoint endpoint = new Endpoint("bar", "baz", 8888);
        final String jsonString = endpoint.toJsonString();

        final Endpoint endpointCopy = Endpoint.fromJson(jsonString);

        assertThat(endpointCopy.getName(), is(endpoint.getName()));
        assertThat(endpointCopy.getHost(), is(endpoint.getHost()));
        assertThat(endpointCopy.getPort(), is(endpoint.getPort()));
    }

    @Test
    public void testEquals() {
        final Endpoint a = new Endpoint("foo", "bar", 1);
        final Endpoint b = new Endpoint("foo", "bar", 1);
        assertThat(a.equals(b), is(true));
        assertThat(b.equals(a), is(true));
        assertThat(b.hashCode(), is(a.hashCode()));

        final Endpoint c = new Endpoint("bar", "foo", 1);
        assertThat(a.equals(c), is(false));
        assertThat(b.equals(c), is(false));

        final Endpoint d = new Endpoint("foo", "bar", 2);
        assertThat(a.equals(d), is(false));

        final Endpoint e = new Endpoint("foo", "baz", 1);
        assertThat(a.equals(e), is(false));

        assertThat(a.equals(null), is(false));
        assertThat(a.equals("some string"), is(false));
    }

    @Test (expected = IllegalArgumentException.class)
    public void testNullName() {
        new Endpoint(null, "foo", 0);
        fail("Constructor should have thrown exception for null name");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testEmptyName() {
        new Endpoint("", "foo", 0);
        fail("Constructor should have thrown exception for null name");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testNullHost() {
        new Endpoint("foo", null, 0);
        fail("Constructor should have thrown exception for null host");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testEmptyHost() {
        new Endpoint("foo", "", 0);
        fail("Constructor should have thrown exception for null host");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testZeroPort() {
        new Endpoint("foo", "bar", 0);
        fail("Constructor should have thrown exception for 0 port");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testNegativePort() {
        new Endpoint("foo", "bar", -1);
        fail("Constructor should have thrown exception for 0 port");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidName() {
        new Endpoint("æøå", "bar", 80);
        fail("Constructor should have thrown exception for 0 port");
    }
}
