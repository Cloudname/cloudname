package org.cloudname.con;

import org.cloudname.con.servlet.MonitorServlet;
import org.cloudname.con.servlet.RootServlet;
import org.cloudname.con.servlet.SystemPropertiesServlet;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.component.LifeCycle;

import java.io.IOException;

import java.net.DatagramSocket;
import java.net.ServerSocket;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;


/**
 * This class implements an embedded web console.
 *
 * For the time being we use Jetty as the HTTP server for this, but
 * this may not be a brilliant idea since it can clash with other uses
 * of Jetty.  We are considering using a much simpler embedded HTTP
 * server and provide our own handler API, but for now we will
 * experiment with using Jetty and allowing servlets to be added
 * rather than more specialized handlers.
 *
 * A WebConsole may only be started once.  If you have stopped it you
 * cannot restart it again -- you have to create a new instance.
 *
 * @author borud
 */
public class WebConsole {
    private static final Logger log = Logger.getLogger(WebConsole.class.getName());

    // Having a default listen port for WebConsole is a bit silly
    // since there may be more than one service running on the same
    // host with a webconsole.  Meaning: we *will* land in trouble if
    // we depend on the default port.  The application should take
    // care of allocating a port to the web console.  Preferably by
    // runtime command line option.
    private static final int DEFAULT_LISTEN_PORT = 4500;

    // This countdown latch is used to determine when the server is up
    // and running.  It will be counted down by the lifecycle listener
    // on Jetty.
    private final CountDownLatch startupLatch = new CountDownLatch(1);

    // The servlets we wish to add.
    private final Map<String, HttpServlet> servlets = new HashMap<String, HttpServlet>();

    private int port = DEFAULT_LISTEN_PORT;
    private Server server;
    private ServletContextHandler ctx;
    private volatile boolean startCalled = false;

    private WebConsole() {
    }

    /**
     * Register a LifeCycle.Listener to count down the startupLatch.
     */
    private void setupLifeCycleListener() {

        // Hook in a lifecycle listener
        server.addLifeCycleListener(new LifeCycle.Listener() {
                @Override public void lifeCycleStarting(LifeCycle event) {
                }

                @Override public void lifeCycleStarted(LifeCycle event) {
                    log.info("Started WebConsole on port " + port);
                    startupLatch.countDown();
                }

                @Override public void lifeCycleFailure(LifeCycle event, Throwable cause) {
                }

                @Override public void lifeCycleStopping(LifeCycle event) {
                }

                @Override public void lifeCycleStopped(LifeCycle event) {
                    log.info("Stopped WebConsole on port " + port);
                }
            });
    }

    /**
     * Set server instance for WebConsole.
     *
     * @param server the Server instance we wish the WebConsole to
     *   use.
     */
    private WebConsole setServer(Server server) {
        this.server = server;

        return this;
    }

    /**
     * Set ServletContextHandler for WebConsole.
     *
     * @param the ServletContextHandler we wish the WebConsole to use.
     */
    private WebConsole setServletContextHandler(ServletContextHandler ctx) {
        this.ctx = ctx;

        return this;
    }

    /**
     * Create a WebConsole instance and populate it with the default
     * system servlets.
     *
     * @param port the port the server should be listening on.
     * @throws PortInUseException
     */
    public static WebConsole create(int port) throws PortInUseException {
        WebConsole console = new WebConsole();
        console.port = port;

        if (!isPortAvailable(port)) {
            throw new PortInUseException();
        }

        // Create server with default connectors and a given port
        // number.
        Server s = new Server(port);
        console.setServer(s);

        // Set up lifecycle listener
        console.setupLifeCycleListener();

        // Create a context handler with no session handling and no
        // security handling.  We will have to revisit this code at
        // some stage later to evaluate if we need to add a security
        // handler.
        ServletContextHandler ctxt = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        ctxt.setContextPath("/");
        s.setHandler(ctxt);
        console.setServletContextHandler(ctxt);

        // Add system servlets.
        console.addServlet(new RootServlet(console), "/");
        console.addServlet(new MonitorServlet(), "/varz/*");
        console.addServlet(new SystemPropertiesServlet(), "/propz/*");

        return console;
    }

    /**
     * Check if a given port is free.
     *
     * @param port
     * @return true if free, false if taken
     */
    public static boolean isPortAvailable(int port) {
        ServerSocket ss = null;
        DatagramSocket ds = null;

        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);

            return true;
        } catch (IOException e) {
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }

        return false;
    }

    /**
     * @return the port number the
     */
    public int getPort() {
        return port;
    }

    /**
     * Start the WebConsole.
     *
     * @throws Exception.  This is intended to be used from the main()
     *   method of a class and if something fails during
     *   initialization we want to fail fast.  Not a lot we can do
     *   about exceptions that occur here.
     */
    public WebConsole start() throws Exception {
        if (startCalled) {
            throw new IllegalStateException("start() called more than once");
        }

        // Fire up the server
        startCalled = true;
        server.start();
        return this;
    }

    /**
     * Wait until the server has started up properly.
     *
     * This method can be useful in unit tests to ensure that the
     * WebConsole is properly up before continuing.
     *
     * @throws InterruptedException
     */
    public WebConsole waitUntilStarted() throws InterruptedException {
        startupLatch.await();
        return this;
    }

    /**
     * Add a servlet with a given path.  The servlets can only be
     * added before the start() method is called.  Unlike servlet
     * containers we do not leave it up to the container to
     * instantiate the servlet: we only accept instances, not classes.
     *
     * @param servlet the servlet instance we wish to add to the web
     *   console.
     * @param path the path at which we wish to publish the servlet
     * @throws NullPointerException if {@code servlet} or {@code path} is null
     * @throws IllegalArgumentException of user tries to add servlet with path "/" or "/*"
     * @throws IllegalStateException if the path the user tries to add already has been added
     */
    public WebConsole addServlet(HttpServlet servlet, String path) {

        if (null == path) {
            throw new NullPointerException("path was null");
        }

        if (null == servlet) {
            throw new NullPointerException("servlet was null");
        }

        // Make sure developers do not inject a root servlet.
        if ("/".equals(path) || path.startsWith("/*")) {

            if (!(servlet instanceof RootServlet)) {
                throw new IllegalArgumentException("You are not allowed to add a root servlet");
            }
        }

        // Note that this check does not catch paths that will overlap
        // due to glob expressions, so it offers only weak protection
        // against developer screw-up.
        if (servlets.containsKey(path)) {
            throw new IllegalStateException("Servlet path already added: " + path);
        }

        // Add servlet to server
        ctx.addServlet(new ServletHolder(servlet), path);

        // Store servlet info
        servlets.put(path, servlet);

        return this;
    }

    /**
     * Return sorted list of servlet paths registered with this server.
     */
    public Set<String> getServletPaths() {
        return new TreeSet<String>(servlets.keySet());
    }

    /**
     * Shut the web console down.
     *
     * @throws Exception.  Not a lot we can do about exceptions that
     *    occur here.
     */
    public WebConsole shutdown() throws Exception {
        server.stop();
        server.join();
        return this;
    }
}
