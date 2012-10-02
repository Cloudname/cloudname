package org.cloudname.example.restapp.rs;

import static org.junit.Assert.assertEquals;

import javax.ws.rs.core.Response.Status;

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

    @Test
    public void access_forbidden_if_no_credentials_provided() throws Exception {

        final ClientResponse response =
                resource().path("/secure-resource").get(ClientResponse.class);

        assertHttpStatus(Status.FORBIDDEN, response);
    }

    @Test
    public void unauthorized_if_wrong_password() throws Exception {

        setAuthentication(TEST_USER, INVALID_PASSWORD);

        final ClientResponse response =
                resource().path("/secure-resource").get(ClientResponse.class);

        assertHttpStatus(Status.UNAUTHORIZED, response);
    }

    @Test
    public void unauthorized_if_wrong_role() throws Exception {

        setAuthentication(TEST_USER2_WRONG_ROLE, TEST_PASSWORD2);

        final ClientResponse response =
                resource().path("/secure-resource").get(ClientResponse.class);

        // The filter throws 403 even though it should ratherthrow 401 (unauthorized)
        assertHttpStatus(Status.FORBIDDEN, response);
    }

    @Test
    public void access_granted_if_correct_credentials_and_role() throws Exception {

        setAuthentication(TEST_USER, TEST_PASSWORD);

        final ClientResponse response =
                resource().path("/secure-resource").get(ClientResponse.class);

        assertHttpStatus(Status.OK, response);

        String responseBody = response.getEntity(String.class);

        assertEquals("Behold my secret beauty!", responseBody);
    }

    private void setAuthentication(String username, String password) {
        client().addFilter(new HTTPBasicAuthFilter(username, password));
    }

}
