package org.cloudname.log.recordstore;

import org.cloudname.log.pb.Timber;

import java.io.InputStream;
import java.io.IOException;

/**
 * This class provides a wrapper for reading Timber.LogEvents from an
 * InputStream.
 *
 * @author borud
 */
public class RecordReader {
    private InputStream in;

    /**
     * @param in the InputStream from which we wish to read
     *  Timber.LogEvent instances from.
     */
    public RecordReader(InputStream in) {
        this.in = in;
    }

    /**
     * Read one Timber.LogEvent instance from the input stream.
     *
     * @return a Timber.LogEvent on success and {@code null} if we
     *   have reached the end of the stream.
     */
    public Timber.LogEvent read() throws IOException {
        return Timber.LogEvent.parseDelimitedFrom(in);
    }

    /**
     * Close the input stream.
     */
    public void close() throws IOException {
        in.close();
    }
}
