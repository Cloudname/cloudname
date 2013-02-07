package org.cloudname.timber.server;

import org.cloudname.timber.server.handler.archiver.SimpleArchiver;
import org.cloudname.timber.common.Constants;

import org.cloudname.flags.Flag;
import org.cloudname.flags.Flags;

/**
 * Main entry point for Timber server.
 *
 * @author borud
 */
public class Main {
    @Flag (name="enable-archiver-plugin", description="Enable the archiver")
    public static boolean enableArchiver = true;

    @Flag (name="logdir", description="the log directory", required=false)
    public static String logdir = "logs";

    @Flag (name="max-slot-size", description = "The max size of individual slot files", required=false)
    public static int maxSlotSize = Constants.DEFAULT_MAX_ARCHIVER_FILESIZE;

    @Flag (name="port", description="The port the logserver listens to", required=false)
    public static int port = Constants.DEFAULT_TIMBER_PORT;

    /**
     * Start the timber server.
     */
    public static void main(String[] args)
        throws Exception
    {
        // Parse the flags.
        Flags flags = new Flags()
            .loadOpts(Main.class)
            .parse(args);

        // Check if we wish to print out help text
        if (flags.helpFlagged()) {
            flags.printHelp(System.out);
            return;
        }

        // Create a server instance
        Server server = new Server(port);

        // Figure out what built-in handlers to populate it with
        // Do we have options for the Archiver plugin?
        if (enableArchiver) {
            SimpleArchiver simpleArchiver = new SimpleArchiver(logdir, "timberserver", maxSlotSize);
            simpleArchiver.init();
            server.addHandler(simpleArchiver);
        }

        // Fire up the server
        server.start();
    }
}
