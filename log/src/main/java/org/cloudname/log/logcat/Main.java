package org.cloudname.log.logcat;

import static org.cloudname.log.pb.Timber.LogEvent;
import static org.cloudname.log.pb.Timber.Payload;

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
            prettyPrint(filename);
        }
    }

    private static void prettyPrint(String filename) throws Exception {
        RecordReader reader = new RecordReader(new FileInputStream(filename));
        LogEvent logEvent = null;
        while ((logEvent = reader.read()) != null) {
            // output
        }
    }
}