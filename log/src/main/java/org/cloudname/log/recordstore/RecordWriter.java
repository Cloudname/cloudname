package org.cloudname.log.recordstore;

import org.cloudname.log.pb.Timber;

import java.io.OutputStream;
import java.io.IOException;


/**
 * Write Timber.LogEvent records to an OutputStream.
 *
 * @author borud
 */
public class RecordWriter {
    private OutputStream out;

    /**
     * @param out the OutputStream we wish to append records to.
     */
    public RecordWriter(OutputStream out) {
        this.out = out;
    }

    /**
     * Prefix the record with an integer sized length field and write
     * it to the output stream.
     */
    public int write(Timber.LogEvent logEvent) throws IOException {
        logEvent.writeDelimitedTo(out);

        // one would believe that com.google.protobuf.AbstractMessage,
        // from which Message instances and thus Timber.LogEvent
        // derives, was implemented in a manner so that the serialized
        // size was memoized, but this is unfortunately not the case.
        // Therefore we might get some speed benefits by perhaps
        // wrapping the OutputStream in an OutputStream that can count
        // the number of bytes that have passed through it.
        return logEvent.getSerializedSize();
    }

    public void close() throws IOException {
        out.close();
    }

    public void flush() throws IOException {
        out.flush();
    }
}