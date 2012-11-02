package org.cloudname.example.restapp.rs;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.cloudname.example.restapp.server.security.A3ClientInitializer;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

public class ExampleSecuredResourceIT extends AbstractResourceTester {

    /** A valid user with the required role example-admin, known during test runs. @see AuthenticationFilter */
    private static final String TEST_USER = "testUser";
    private static final String TEST_PASSWORD = "testUser_psw";
    private static final String TEST_USER2_WRONG_ROLE = "anotherTestUser";
    private static final String TEST_PASSWORD2 = "anotherTestUser_psw";
    private static final String INVALID_PASSWORD = "this psw isn't valid!";

    @BeforeClass
    public static void setUpA3Client() throws IOException {
        A3ClientInitializer.setUserDbPathForTesting("/test-clients.json");
        A3ClientInitializer.tryInitializeA3Client();
    }

    @Test
    public void unahtorizedIfNoCredentials() throws Exception {

        final ClientResponse response =
                resource().path("/secure-resource").get(ClientResponse.class);

        assertThat(response.getClientResponseStatus(), is(ClientResponse.Status.UNAUTHORIZED));
    }

    @Test
    public void unahtorizedIfWrongPassword() throws Exception {

        setAuthentication(TEST_USER, INVALID_PASSWORD);

        final ClientResponse response =
                resource().path("/secure-resource").get(ClientResponse.class);

        assertThat(response.getClientResponseStatus(), is(ClientResponse.Status.UNAUTHORIZED));
    }

    @Test
    public void unahtorizedIfWrongRole() throws Exception {

        setAuthentication(TEST_USER2_WRONG_ROLE, TEST_PASSWORD2);

        final ClientResponse response =
                resource().path("/secure-resource").get(ClientResponse.class);

        assertThat(response.getClientResponseStatus(), is(ClientResponse.Status.FORBIDDEN));
    }

    @Test
    public void okIfGoodCredentials() throws Exception {

        setAuthentication(TEST_USER, TEST_PASSWORD);

        final ClientResponse response =
                resource().path("/secure-resource").get(ClientResponse.class);

        assertThat(response.getClientResponseStatus(), is(ClientResponse.Status.OK));

        String responseBody = response.getEntity(String.class);

        assertEquals("Behold my secret beauty!", responseBody);
    }

    private void setAuthentication(String username, String password) {
        client().addFilter(new HTTPBasicAuthFilter(username, password));
    }

}
