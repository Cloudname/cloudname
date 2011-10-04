package org.cloudname.base;

import org.cloudname.con.WebConsole;

import org.cloudname.Coordinate;

import java.net.ServerSocket;
import java.net.SocketException;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Initialization helper for applications and services.  This class is
 * used to help set up logging and the web console.
 *
 * @author borud
 */
public class Base {
    private static final Logger log = Logger.getLogger(Base.class.getName());

    private Coordinate coordinate;
    private WebConsole webConsole;
    private int webConsolePort = 0;
    private boolean loggingSetup = false;

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
     * Explicitly set the WebConsole port.
     *
     * @param webconsolePort the web console port.
     */
    public Base setWebConsolePort(int port) {
        webConsolePort = port;
        return this;
    }

    /**
     * Turn off or on the loghandler setup.  If you do not explicitly
     * set this it will default to {@code false} which means that it
     * won't touch the logging setup.
     *
     * @param loggingSetup if {@code true} then we set up our
     *   log handler, if {@code false} we leave the logging alone.
     */
    public Base logHandlerSetup(boolean loggingSetup) {
        this.loggingSetup = loggingSetup;
        return this;
    }

    /**
     * Initialize the base features.
     */
    public void init() {
        // Set up the WebConsole
        if (webConsolePort == 0) {
            webConsolePort = getFreePort();
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