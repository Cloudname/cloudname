package org.cloudname.log.logcat;

import org.cloudname.log.format.CompactFormatter;
import org.cloudname.flags.Flag;
import org.cloudname.flags.Flags;
import java.io.FileInputStream;
import java.util.List;

/**
 * Utility for printing logs.
 *
 * @author borud
 */
public class Main {
    private static final int DEFAULT_TAIL_DELAY_MS = 50;

    @Flag(name = "follow", description = "Follow log file")
    private static String tailFile = null;

    public static void main(String[] args) throws Exception {
        Flags flags = new Flags()
            .loadOpts(Main.class)
            .parse(args);
        List<String> files = flags.getNonOptionArguments();
        LogCat cat = new LogCat(new CompactFormatter());

        if (null != tailFile) {
            cat.catStream(new TailInputStream(tailFile, DEFAULT_TAIL_DELAY_MS));
        }
        else if (0 == files.size()) {
            cat.catStream(System.in);
        }
        else {
            for (String filename : files) {
                cat.catStream(new FileInputStream(filename));
            }
        }
    }
}
