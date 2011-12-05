package org.cloudname.log.recordstore;

import org.cloudname.log.pb.Timber;

import java.io.OutputStream;
import java.io.IOException;


/**
 * Write Timber.LogEvent records to an OutputStream.
 *
 * This class is not thread safe.
 *
 * @author borud
 */
public class RecordWriter {
    private CountingOutputStream out;

    /**
     * @param out the OutputStream we wish to append records to.
     */
    public RecordWriter(OutputStream out) {
        this.out = new CountingOutputStream(out);
    }

    /**
     * Prefix the record with an integer sized length field and write
     * it to the output stream.
     */
    public int write(Timber.LogEvent logEvent) throws IOException {
        logEvent.writeDelimitedTo(out);
        return out.getLastBytesWritten();
    }

    public void close() throws IOException {
        out.close();
    }

    public void flush() throws IOException {
        out.flush();
    }
}
