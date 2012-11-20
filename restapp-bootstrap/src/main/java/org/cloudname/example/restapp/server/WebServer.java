package org.cloudname.example.restapp.server;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.cloudname.a3.jaxrs.JerseyRoleBasedAccessControlResourceFilterFactory;
import org.cloudname.example.restapp.server.security.AuthenticationFilter;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.sun.jersey.spi.container.servlet.ServletContainer;

/**
 * Programmatic configuration of Jetty to run Jersey and our REST resources.
 */
public final class WebServer {

    /**
     * ;-separated list of packages where Jersey should search for REST resources, providers etc.
     */
    private static final String REST_RESOURCE_PACKAGES = "org.cloudname.example.restapp.rs";

    /**
     * The name of this service.
     */
    private final String serviceName;

    /**
     * The highest legal IP port number.
     */
    private static final int MAX_IP_PORT_NUMBER = 65535;

    private static final Logger log = Logger.getLogger(WebServer.class.getName());

    /**
     * The hostname of this webserver.
     */
    private final String host;

    /**
     * The portnumber of this webserver.
     */
    private final int port;

    /**
     * A directory where a log will be dropped.
     */
    private String requestLogDir;

    /**
     * The Jetty server.
     */
    private final Server server = new Server();

    /**
     * Create a new webserver.
     *
     * @param servicename (required)
     *            the name of the service.
     * @param host (optional)
     *            the hostname, if non-null.
     * @param port (required)
     *            the portnumber.
     */
    public WebServer(
            final String servicename,
            final String host,
            final int port) {

        if (servicename == null) {
            throw new IllegalArgumentException("servicename is required (whatever string to identify the app)");
        }
        if (port <= 0 || port >= MAX_IP_PORT_NUMBER) {
            throw new IllegalArgumentException("Illegal port number " + port + ", should be > 0 and < " + MAX_IP_PORT_NUMBER);
        }

        this.serviceName = servicename;
        this.host = host;
        this.port = port;
    }

    /**
     * Start up the webserver.
     */
    public void start() {

        final SocketConnector connector = new SocketConnector();

        connector.setPort(port);

        if (null != host && !host.equals("")) {
            connector.setHost(host);
        }

        server.setConnectors(new Connector[] { connector });

        final ServletContextHandler idHandler = configureRestHandler();
        final RequestLogHandler requestLogHandler = configureRequestLoggerIfDesired();

        final HandlerCollection handlers = new HandlerCollection();

        if (null == requestLogHandler) {
            handlers.setHandlers(new Handler[] { idHandler });
        } else {
            handlers.setHandlers(new Handler[] {
                    idHandler, requestLogHandler });
        }

        server.setHandler(handlers);

        try {
            log.log(Level.INFO,
                    "Starting " + serviceName + " server on port " + port + " ( http://0.0.0.0:" + port + " )");
            server.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ServletContextHandler configureRestHandler() {
        final ServletContextHandler idHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        final ServletHolder restServlet = new ServletHolder(ServletContainer.class);
        configureJerseyParams(restServlet);
        idHandler.addServlet(restServlet, "/*");
        return idHandler;
    }

    private void configureJerseyParams(final ServletHolder restServlet) {
        // Setting package path where Jersey looks for Providers and Resources
        restServlet.setInitParameter("com.sun.jersey.config.property.packages",
                REST_RESOURCE_PACKAGES +
                ";org.codehaus.jackson.jaxrs" +
                ";org.cloudname.a3.jaxrs"); // A3 authentication exception mapper
        // Allows jackson to map from JAXB annotated pojo's to JSON
        restServlet.setInitParameter("com.sun.jersey.api.json.POJOMappingFeature", "true");
        // Disabling WADL because it's not very REST-like
        restServlet.setInitParameter("com.sun.jersey.config.feature.DisableWADL", "false");
        configureAuthenticationAndAuthorization(restServlet);
    }

    private void configureAuthenticationAndAuthorization(final ServletHolder restServlet) {
        // Active our authentication filter:
        restServlet.setInitParameter("com.sun.jersey.spi.container.ContainerRequestFilters"
                , AuthenticationFilter.class.getName());
        // Enable @RolesAllowed:
        restServlet.setInitParameter("com.sun.jersey.spi.container.ResourceFilters"
                , JerseyRoleBasedAccessControlResourceFilterFactory.class.getName());
    }

    private RequestLogHandler configureRequestLoggerIfDesired() {
        RequestLogHandler requestLogHandler = null;

        if (null != requestLogDir) {
            final File logdir = new File(requestLogDir);
            if (!logdir.exists()) {
                throw new IllegalArgumentException(
                        "Logdir doesn't exist: " + logdir);
            }

            if (!logdir.isDirectory()) {
                throw new IllegalArgumentException(
                        "Logdir exists, but isn't a directory: " + logdir);
            }

            if (!logdir.canWrite()) {
                throw new IllegalArgumentException(
                        "Logdir exists, but  can't be written to:  " + logdir);
            }

            requestLogHandler = new RequestLogHandler();
            final NCSARequestLog requestLog =
                    new NCSARequestLog(requestLogDir
                            + "/" + serviceName
                            + "-yyyy_mm_dd.request.log");
            requestLog.setRetainDays(90);
            requestLog.setAppend(true);
            requestLog.setExtended(false);
            requestLog.setLogTimeZone("GMT");
            requestLogHandler.setRequestLog(requestLog);
        }
        return requestLogHandler;
    }

    /**
     * Shut down the web server.
     */
    public void shutdown() {
        try {
            server.stop();
            server.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
