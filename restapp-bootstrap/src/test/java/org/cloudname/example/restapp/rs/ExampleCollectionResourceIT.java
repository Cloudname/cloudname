package org.cloudname.example.restapp.rs;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.sun.jersey.api.client.ClientResponse;

/**
 * Example integration test of a REST resource.
 * <p>
 *  Its name ends with *IT so that Infinitest/Surefire don't run it all the time
 *  (because it is slow). It is executed by Failsafe or manually.
 * </p>
 * @author jholy
 */
public class ExampleCollectionResourceIT extends AbstractResourceTester {

    @Test
    public void getWorks() throws Exception {

        final ClientResponse response =
                resource().path("/").get(ClientResponse.class);

        assertThat(response.getClientResponseStatus(), is(ClientResponse.Status.OK));

        String responseBody = response.getEntity(String.class);
        // Note: getEntity returns null in subsequent calls

        assertEquals("There is not much to show, buddy", responseBody);
    }

}
