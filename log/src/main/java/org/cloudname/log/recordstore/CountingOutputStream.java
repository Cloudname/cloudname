package org.cloudname.log.recordstore;

import java.io.OutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;

/**
 * A proxying OutputStream that provides us with the means of counting
 * the number of bytes written to an OutputStream.
 *
 * This class is not thread safe.
 *
 * @author borud
 */
public class CountingOutputStream extends FilterOutputStream {
    private long bytesWritten = 0L;
    private int lastBytesWritten = 0;

    /**
     * Creates an OutputStream capable of wrapping another
     * OutputStream and counting the number of bytes written through
     * it.
     *
     * @param os The OutputStream you wish to wrap.
     */
    public CountingOutputStream(OutputStream os) {
        // This is ugly, os is stored in protected superclass variable "out".
        super(os);
    }

    /**
     * Return the number of bytes written to this OutputStream.
     *
     * @return number of bytes written.
     */
    public long getBytesWritten() {
        return bytesWritten;
    }

    /**
     * Get the number of bytes written by last write.
     *
     * @return number of bytes written by last write.
     */
    public int getLastBytesWritten() {
        return lastBytesWritten;
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
        ++bytesWritten;
        lastBytesWritten = 1;
    }

    @Override
    public void write(byte[] b) throws IOException {
        // For some reason other implementations I've seen deal with
        // the case when b is null even though some OutputStream
        // implementations will throw a NullPointerException.
        int len = (null == b) ? 0 : b.length;
        out.write(b);
        lastBytesWritten = len;
        bytesWritten += len;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);

        lastBytesWritten = len;
        bytesWritten += len;
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }
}
