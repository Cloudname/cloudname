package org.cloudname.timber.server;

import org.cloudname.timber.server.handler.archiver.SimpleArchiver;
import org.cloudname.timber.common.Constants;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


/**
 * Main entry point for Timber server.
 *
 * @author borud
 */
public class Main {
    private static OptionParser parser = new OptionParser();

    private static OptionSpec<Void> help
        = parser.accepts("help", "this help text");

    private static OptionSpec<String> logdir
        = parser.accepts("logdir", "the root log directory for SimpleArchiver")
        .withRequiredArg().ofType(String.class);

    private static OptionSpec<Integer> maxSlotSize
        = parser.accepts("maxslotsize", "max file size for SimpleArchiver files")
        .withRequiredArg().ofType(Integer.class)
        .defaultsTo(Constants.DEFAULT_MAX_ARCHIVER_FILESIZE);

    private static OptionSpec<Integer> port
        = parser.accepts("port", "listen port for log server")
        .withRequiredArg().ofType(Integer.class)
        .describedAs("1-65535")
        .defaultsTo(Constants.DEFAULT_TIMBER_PORT);

    public static void main(String[] args)
        throws Exception
    {
        // Parse command line options
        OptionSet optionSet = parser.parse(args);

        // Check if we wish to print out help text
        if (optionSet.has(help)) {
            parser.printHelpOn(System.out);
            return;
        }

        // Create a server instance
        Server server = new Server(port.value(optionSet));

        // Figure out what built-in handlers to populate it with
        // Do we have options for the Archiver plugin?
        if (optionSet.has(logdir)) {
            SimpleArchiver simpleArchiver
                = new SimpleArchiver(logdir.value(optionSet),
                                     maxSlotSize.value(optionSet));
            simpleArchiver.init();
            server.addHandler(simpleArchiver);
        }

        // Fire up the server
        server.start();
    }

}