package org.cloudname.base;

import org.cloudname.con.WebConsole;
import org.cloudname.log.ComboHandler;

import org.cloudname.Coordinate;


import java.net.ServerSocket;
import java.net.SocketException;

import java.io.File;
import java.io.IOException;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.Handler;

/**
 * Initialization helper for applications and services.  This class is
 * used to help set up logging and the web console.
 *
 * @author borud
 */
public class Base {
    private static final Logger log = Logger.getLogger(Base.class.getName());

    private Coordinate coordinate = null;
    private WebConsole webConsole;
    private ComboHandler comboHandler;
    private boolean enableWebConsole = true;
    private int webConsolePort = 0;
    private boolean setupLogging = false;
    private String logPath = null;

    /**
     * Set the primary coordinate for this service or application.
     *
     * @param coordinateString the primary coordinate of this service or application.
     */
    public Base setCoordinate(String coordinateString) {
        coordinate = Coordinate.parse(coordinateString);
        return this;
    }

    /**
     * Get the primary coordinate of this service.
     *
     * @return the primary coordinate of this service.
     */
    public Coordinate getCoordinate() {
        return coordinate;
    }

    /**
     * Explicitly set the WebConsole port.
     *
     * @param webconsolePort the web console port.
     */
    public Base setWebConsolePort(int port) {
        webConsolePort = port;
        return this;
    }

    /**
     * @return the WebConsole port of this service.  Note that if no
     *   port was set explicitly prior to calling the init() method,
     *   the value of this port will be 0 before init() is called and
     *   it will have a randomly assigned port number after the method
     *   has been called.
     */
    public int getWebConsolePort() {
        return webConsolePort;
    }

    /**
     * Set the root path for logging explicitly.
     *
     * @param logPath the root logging directory.
     */
    public Base setLogPath(String logPath) {
        this.logPath = logPath;
        return this;
    }

    /**
     * @return the log path of this service. Note that if no log path
     * was set prior to calling init() the log path will be set to a
     * default log path, which is the "logs" directory under the
     * current working directory.
     */
    public String getLogPath() {
        return logPath;
    }

    /**
     * Turn off or on the loghandler setup.  If you do not explicitly
     * set this it will default to {@code false} which means that it
     * won't touch the logging setup.
     *
     * @param setupLogging if {@code true} then we set up our
     *   log handler, if {@code false} we leave the logging alone.
     */
    public Base logHandlerSetup(boolean setupLogging) {
        this.setupLogging = setupLogging;
        return this;
    }


    /**
     * Enable or disable the WebConsole.  By default the web console
     * is enabled.
     *
     * @param enableWebConsole if {@code true} we enable the web
     *   console, if set to {@code false} we do not enable the web
     *   console.
     */
    public Base setEnableWebConsole(boolean enableWebConsole) {
        this.enableWebConsole = enableWebConsole;
        return this;
    }


    /**
     * Initialize the base features.
     *
     * @throws Exception since this is designed to be used from the
     *   main() method any exception thrown during setup should just
     *   be allowed to trickle up and terminate things early.  There
     *   is no point in wasting time handling exceptions we can't do
     *   anything about.
     */
    public void init() throws Exception {
        initWebConsole();
        initLogging();
    }

    /**
     * Initialize the WebConsole.
     */
    private void initWebConsole() throws Exception {
        if (! enableWebConsole) {
            return;
        }

        // If we did not set an explicit port for the webconsole we
        // allocate a random port for the web console.
        if (webConsolePort == 0) {
            webConsolePort = getFreePort();
        }

        webConsole = WebConsole.create(webConsolePort).start();
    }

    /**
     * Initialize the logging setup.  This is a pig of a method since
     * it messes programmatically with the log handler setup rather
     * than doing things like the JUL designers intended (set the
     * handler via properties).
     *
     * Note that this method will mutate the {@code logPath} if it is
     * {@code null} and set it to whatever it chooses as the log path.
     * Right now it just sets it to a directory called "logs" under
     * the current directory.
     */
    private void initLogging() {
        if (! setupLogging) {
            return;
        }

        // If we do not have a valid coordinate we just set the
        // coordinate to an empty string.  The ComboHandler doesn't
        // use the coordinate for anything else than just inserting it
        // into log messages.
        String coord = (null == coordinate) ? "" : coordinate.asString();

        // If the logPath was not set we just set it to the "log"
        // directory in the current directory.
        if (null == logPath) {
            logPath = new File("." + File.separatorChar + "logs").getAbsolutePath();
        }

        // Now create the handler.
        comboHandler = new ComboHandler(coord, logPath);

        // This is where things get ugly.
        // First we remove all log handlers
        Logger rootLogger = Logger.getLogger("");
        for (Handler handler : rootLogger.getHandlers()) {
            rootLogger.removeHandler(handler);
        }

        // Then we add the comboHandler.
        rootLogger.addHandler(comboHandler);
    }

    /**
     * Shut down whatever things we have started.
     */
    public void shutdown() throws Exception {
        if (null != webConsole) {
            webConsole.shutdown();
        }

        if (null != comboHandler) {
            // Not sure if we should close it so we'll just flush it.
            comboHandler.flush();
        }
    }

    /**
     * Find a free local port.  Strictly speaking I suspect this
     * method of doing things has all sorts of race conditions but
     * since ports allocated this way have a tendency to increment on
     * the platforms I have tested it on it should for the most part
     * work.
     *
     * (This method is essentially a duplicate of the
     * Net.getFreePort() method in testtools, but I would rather
     * duplicate a short method than introduce a runtime dependency on
     * testtools).
     *
     * @return a free local port number or 0 if something went wrong.
     */
    private static int getFreePort() {
        int freePort = 0;
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            log.log(Level.INFO, "Failed to allocate local port", e);
            return 0;
        } finally {
            try {
                if (null != socket) {
                    socket.close();
                }
            } catch (IOException e) {
                log.warning("Failed to close server socket used for finding free port");
                return 0;
            }
        }
    }
}