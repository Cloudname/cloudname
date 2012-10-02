package org.cloudname.example.restapp.server;

import org.cloudname.flags.Flag;

import com.comoyo.base.Base;

/**
 * Main entry point when starting the server from the command line
 * (which is the default way of starting it).  The default port on which
 * it will start is 8080, and if the flag "localhost-only" is set, it
 * will only accept connections from localhost.
 */
public final class Main {

    /**
     * Utility class, not for instantiation.
     */
    private  Main() {
    }

    /**
     * The name of this service, as we announce it to the world.
     */
    private static final  String SERVICE_NAME = "Sample Cloudname-Based REST App";


    /**
     * The port we will accept connections on.
     */
    @Flag(name = "port", description = "server port")
    private static int locationPort = 8080;


    /**
     * A flag that is true iff we will only start the service on
     * localhost and nowhere else.
     */
    @Flag(name = "localhost-only", description = "Start as localhost only (true/false)")
    public static boolean locationPlaintextLocalhostOnly = false;

    public static void main(final String [] args) throws Exception {

        final Base base = new Base();
        base.loadOpts(Main.class).parseFlags(args);

        if (base.helpFlagged()) {
            base.printHelp(System.out);
            return;
        }

        if (base.versionFlagged()) {
            base.printVersion(System.out);
            return;
        }

        base.init();

        WebServer webServer = new WebServer(
                SERVICE_NAME,
                locationPlaintextLocalhostOnly ? "127.0.0.1" : null,
                locationPort);
        webServer.start();
    }
}
