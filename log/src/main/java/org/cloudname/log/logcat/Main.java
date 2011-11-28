package org.cloudname.log.logcat;

import static org.cloudname.log.pb.Timber.LogEvent;
import static org.cloudname.log.pb.Timber.Payload;

import org.cloudname.log.format.LogEventFormatter;
import org.cloudname.log.format.SingleLineFormatter;

import org.cloudname.log.recordstore.RecordReader;

import java.io.FileInputStream;

/**
 * Utility for printing logs.
 *
 * @author borud
 */
public class Main {
    public static void main(String[] args) throws Exception {
        for (String filename : args) {
            prettyPrint(filename, new SingleLineFormatter(), true);
        }
    }

    private static void prettyPrint(String filename, LogEventFormatter formatter, boolean tail)
        throws Exception
    {
        RecordReader reader = new RecordReader(new FileInputStream(filename));
        while (true) {
            LogEvent logEvent = reader.read();

            if (logEvent != null) {
                System.out.println(formatter.format(logEvent));
                continue;
            }

            // Invariant: read() returned null.  If we are in tail mode
            // we simply sleep a bit and then try again.
            if (tail) {
                Thread.sleep(200);
                continue;
            }

            // Invariant: read() returned null and we are not in tail mode
            reader.close();
            return;
        }
    }
}