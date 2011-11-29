package org.cloudname.log.logcat;

import org.cloudname.log.format.CompactFormatter;
import java.io.FileInputStream;

/**
 * Utility for printing logs.
 *
 * @author borud
 */
public class Main {
    public static void main(String[] args) throws Exception {
        LogCat cat = new LogCat(new CompactFormatter());
        for (String filename : args) {
            cat.catStream(new FileInputStream(filename));
        }
    }
}
