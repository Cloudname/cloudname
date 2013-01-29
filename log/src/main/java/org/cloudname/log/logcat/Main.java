package org.cloudname.log.logcat;

import org.cloudname.flags.Flag;
import org.cloudname.flags.Flags;
import org.cloudname.log.format.CompactFormatter;
import org.cloudname.log.format.FullFormatter;
import org.cloudname.log.format.LogEventFormatter;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

/**
 * Utility for printing logs.
 *
 * @author borud
 */
public class Main {
    private static final int DEFAULT_TAIL_DELAY_MS = 50;

    public enum LogFormatter {
        COMPACT,
        FULL,
        PLUGIN,
        NONE
    }

    @Flag(name = "follow", description = "Follow log file (tail). Only supports one file at a time.")
    private static String tailFile = null;

    @Flag(name = "format", description = "The type of formatting to use (compact/full).")
    private static String formatType = "compact";

    @Flag(name = "formatter-plugin-class", description = "Formatter class to load. Attempt to load this class from" +
        "classpath as a LogEventFormatter.")
    private static String pluginClass = "org.cloudname.MyFormatter";

    public static void main(final String[] args) throws Exception {
        final Flags flags = new Flags()
            .loadOpts(Main.class)
            .parse(args);
        final List<String> files = flags.getNonOptionArguments();

        if (flags.helpFlagged() || args.length == 0) {
            System.out.print("\nUsage 'java -jar <jarfile> <options> <filename(s)>");

            flags.printHelp(System.out);
            return;
        }

        final LogCat cat;
        switch (toLogFormatter(formatType)) {
            case COMPACT:
                cat = new LogCat(new CompactFormatter());
                break;
            case FULL:
                cat = new LogCat(new FullFormatter());
                break;
            case PLUGIN:
                LogEventFormatter pluginFormatter = (LogEventFormatter) Class.forName(pluginClass).newInstance();
                cat = new LogCat(pluginFormatter);
                break;
            default:
                System.out.println("Unknown log formatter. Exiting...");
                return;
        }

        if (null != tailFile) {
            cat.catStream(new TailInputStream(tailFile, DEFAULT_TAIL_DELAY_MS));
        }
        else if (0 == files.size()) {
            cat.catStream(System.in);
        }
        else {
            for (final String filename : files) {
                try {
                    cat.catStream(new FileInputStream(filename));
                } catch (Exception e) {
                    throw new Exception("Error while attempting to read from " + filename, e);
                }
            }
        }
    }

    /**
     * Helper method to uppercase and catch nullpointers.
     *
     * @param str The string to match.
     * @return returns the matching enum.
     */
    public static LogFormatter toLogFormatter(final String str)
    {
        try {
            return LogFormatter.valueOf(str.toUpperCase());
        }
        catch (Exception ex) {
            return LogFormatter.NONE;
        }
    }
}
