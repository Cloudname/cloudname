package org.cloudname.log.logcat;

import org.cloudname.log.pb.Timber;
import org.cloudname.log.recordstore.RecordReader;

import org.cloudname.log.format.LogEventFormatter;
import org.cloudname.log.format.SingleLineFormatter;

import java.io.InputStream;


/**
 * Class for formatting a stream of varint frames containing
 * Timber.LogEvent instances.
 *
 * @author borud
 */
public class LogCat {
    private LogEventFormatter formatter;

    /**
     * Construct a LogCat instance which uses a default {@see
     * LogEventFormatter} implementation ({@see SingleLineFormatter}).
     */
    public LogCat() {
        this(new SingleLineFormatter());
    }

    /**
     * Construct a LogCat instance given an instance of a {@see LogEventFormatter}.
     *
     * @param formatter an instance of a {@see LogEventFormatter}.
     */
    public LogCat(LogEventFormatter formatter) {
        this.formatter = formatter;
    }

    /**
     * Read log stream from {@code input}, format log messages using a
     * formatter.
     *
     * @param input the InputStream we wish to read from
     */
    public void catStream(InputStream input) throws Exception {
        RecordReader reader = new RecordReader(input);
        try {
            Timber.LogEvent logEvent = null;
            while ((logEvent = reader.read()) != null) {
                System.out.println(formatter.format(logEvent));
            }
        } finally {
            reader.close();
        }
    }
}
