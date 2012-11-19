package org.cloudname.log.logcat;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.channels.FileChannel;


/**
 * Class for progressively returning data written to the file being
 * read, blocking to wait for new data if necessary.
 *
 * @author argggh
 */
public class TailInputStream extends InputStream {
    protected FileInputStream stream;
    private int delay;

    /**
     * Construct a TailInputStream instance
     *
     * @param name the name of the file to read from
     * @param delay milliseconds to pause between failing attempts to read
     */
    public TailInputStream(final String name, final int delay)
        throws IOException, FileNotFoundException
    {
        this.stream = new FileInputStream(name);
        this.delay = delay;
        final FileChannel ch = stream.getChannel();
        ch.position(ch.size());
    }

    boolean waitForRead(int resIn, int[] resOut)
    {
        if (resIn == -1) {
            try {
                Thread.sleep(delay);
            }
            catch (java.lang.InterruptedException ignore) {
            }
            return true;
        }
        else {
            resOut[0] = resIn;
            return false;
        }
    }

    @Override
    public int read()
        throws IOException
    {
        final int[] res = new int[1];
        while (waitForRead(stream.read(), res)) {}
        return res[0];
    }

    @Override
    public int read(byte[] b)
        throws IOException
    {
        final int[] res = new int[1];
        while (waitForRead(stream.read(b), res)) {}
        return res[0];
    }

    @Override
    public int read(byte[] b, int off, int len)
        throws IOException
    {
        final int[] res = new int[1];
        while (waitForRead(stream.read(b, off, len), res)) {}
        return res[0];
    }
}
