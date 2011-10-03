package org.cloudname;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for Endpoint.
 *
 * @author borud
 */
public class EndpointTest {
    @Test
    public void testSimple() throws Exception {
        Endpoint endpoint = new Endpoint(Coordinate.parse("1.foo.bar.zot"),
                                         "rest-api",
                                         "somehost",
                                         4711,
                                         "http",
                                         null);
        String json = endpoint.toJson();
        Endpoint endpoint2 = Endpoint.fromJson(json);

        assertEquals(endpoint.getCoordinate(), endpoint2.getCoordinate());
        assertEquals(endpoint.getName(), endpoint2.getName());
        assertEquals(endpoint.getHost(), endpoint2.getHost());
        assertEquals(endpoint.getPort(), endpoint2.getPort());
        assertEquals(endpoint.getEndpointData(), endpoint2.getEndpointData());

        System.out.println(json);
    }
}