package org.cloudname.example.restapp.rs;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.core.Response.Status;

import org.cloudname.example.restapp.server.AuthenticationFilter;
import org.cloudname.testtools.Net;
import org.junit.Before;
import org.junit.BeforeClass;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.container.filter.RolesAllowedResourceFilterFactory;
import com.sun.jersey.test.framework.AppDescriptor;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;
import com.sun.jersey.test.framework.spi.container.grizzly.web.GrizzlyWebTestContainerFactory;

/**
 * Parent class for integration testing REST resources over HTTP, running in an embedded Jersey/container.
 * <p>
 *  It takes care of starting Jersey in a container using a free port and configuring Jersey to
 *  look for resources in the right package and to provide more troubleshooting information in
 *  responses and in a log file.
 * </p>
 *
 * @see #assertHttpStatus(Status, ClientResponse)
 */
public abstract class AbstractResourceTester extends JerseyTest {

    /**
     * The package where the webserver should look for the resources to use when testing.
     */
    private static final String REST_RESOURCE_PACKAGE = AbstractResourceTester.class.getPackage().getName();

    private static int port;

    /**
     * Create a new test resource, hooking it to a Grizzly container using the protocol required by the abstract
     * JerseyTest class.
     */
    public AbstractResourceTester() {
        super(new GrizzlyWebTestContainerFactory());
    }

    @Override
    public final AppDescriptor configure() {
        return new WebAppDescriptor.Builder()
                .initParam(
                        "com.sun.jersey.config.property.packages",
                        REST_RESOURCE_PACKAGE
                                + ";org.codehaus.jackson.jaxrs")
                .initParam("com.sun.jersey.api.json.POJOMappingFeature", "true")
                // Active our authentication filter:
                .initParam("com.sun.jersey.spi.container.ContainerRequestFilters", AuthenticationFilter.class.getName())
                // Enable @RolesAllowed:
                .initParam("com.sun.jersey.spi.container.ResourceFilters", RolesAllowedResourceFilterFactory.class.getName())
                // Add info about request matching to response headers:
                .initParam("com.sun.jersey.config.feature.Trace", "true")
                .build();
    }

    @BeforeClass
    public static void assignFreePort() throws IOException {
        port = Net.getFreePort();
    }

    @Override
    protected final int getPort(int defaultPort) {
        return port;
    }

    /**
     * Configure detailed logging for Jersey to make troubleshooting easier.
     */
    @BeforeClass
    public static void setupJerseyLog() throws Exception {
        String logFileName = AbstractResourceTester.class.getPackage().getName();
        String logDir = System.getProperty("java.io.tmpdir");
        String logPath = logDir + File.separatorChar + logFileName + "_jersey_test.log";
        Logger.getLogger("").addHandler(new FileHandler(logPath));
        Logger.getLogger("com.sun.jersey").setLevel(Level.FINEST);
    }

    /**
     * Make setUp final so that a subclass won't override it by mistake. (It starts the container.)
     */
    @Before
    @Override
    public final void setUp() throws Exception {
        super.setUp();
    }

    /**
     * Verify that the response's status is as expected, provide useful error message if not,
     * including the response body (that might carry some details of the failure).
     *
     * @param expectedStatus
     *            (required) What status you expected; example: {@link Status#OK}
     *            (Use JAX-RS' Response.Status, not the Jersey-specific ClientResponse.Status.)
     * @param actualResponse
     *            (required) The response, from st. like
     *            <code>{@code resource().path("exampleResource").get(ClientResponse.class);}</code>
     */
    public static void assertHttpStatus(final Status expectedStatus, final ClientResponse actualResponse) {
        int actualStatus = actualResponse.getStatus();
        if (actualStatus != expectedStatus.getStatusCode()) {
            String responseBody = actualResponse.getEntity(String.class);
            fail("Expected status " + expectedStatus.getStatusCode() + " but got " + actualStatus
                    + "; response body: '" + responseBody + "'" + "\nheaders:" + actualResponse.getHeaders());
        }

    }
}
