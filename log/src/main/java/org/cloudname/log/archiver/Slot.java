package org.cloudname.log.archiver;

import org.cloudname.log.pb.Timber;
import org.cloudname.log.recordstore.RecordWriter;


import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * This class takes care of writing log messages to a "slot".  A slot
 * can consist of one or more files.  Once a slot file reaches maximum
 * file size we begin to populate the next file.  Slot files are
 * numbered with an increasing sequence number.  This produces a sequence
 * of files of the form:
 *
 * <ul>
 *   <li> &lt;slotDir&gt&lt;prefix&gt;-1
 *   <li> &lt;slotDir&gt&lt;prefix&gt;-2
 *   <li> ...
 *   <li> &lt;slotDir&gt&lt;prefix&gt;-10
 *   <li> ...
 * </ul>
 *
 * @author borud
 */
public class Slot {
    private static final Logger log = Logger.getLogger(Slot.class.getName());

    // When resuming an existing file we skip to the next sequence file if we have
    // already logged more than RESUME_LIMIT_PERCENT percent of maxSize
    private static final int RESUME_LIMIT_PERCENT = 90;

    // Some ad-hoc benchmarking suggests that even just 1k of buffer
    // space helps.  Not much to be gained from a larger buffer for a
    // single slot, but we bump the size a bit since there can be
    // several slots active at the same time to lower the number of
    // disk IO operations.
    private static final int DEFAULT_OUTPUT_BUFFER_SIZE = 20 * 1024;

    private final String prefix;
    private final long maxSize;
    private final long resumeLimit;
    private final int outputBufferSize = DEFAULT_OUTPUT_BUFFER_SIZE;

    private int slotSequenceCount = 0;
    private File currentFile = null;
    private RecordWriter currentWriter = null;
    private int writeCountdown;
    private boolean closed = false;

    // Write count for current slot file
    private int writeCount = 0;

    // Keeps track of number of bytes in file
    private long numBytesInFile = 0;

    /**
     * Create a Slot.
     *
     * @param prefix the prefix for the slot file
     * @param maxSize the maximum allowed file size in bytes for individual slot files
     */
    public Slot(String prefix, long maxSize) {
        this.prefix = prefix;
        this.maxSize = maxSize;

        resumeLimit = (maxSize * RESUME_LIMIT_PERCENT) / 100;
    }

    /**
     * @return the slot file path for a given sequence number.
     */
    private String nameForSequenceNo(int slotSequence) {
        return prefix + "_" + slotSequence;
    }

    /**
     * Check if the prospective filename exists with a suffix
     * indicating it has been compressed.
     *
     * @param filename the name for which we want to check if there is
     *   a compressed version.
     * @return {@code true} if a compressed file of this slot exists,
     *   {@code false} otherwise.
     */
    private static boolean compressedSlotExists(String filename) {
        if (new File(filename + ".gz").exists()
            || new File(filename + ".bz2").exists()) {
            return true;
        }

        return false;
    }

    /**
     * Find the next slot file to write to.
     *
     * @return a File instance that points to the next slot file
     */
    private File findNextSlotFile() throws IOException {
        while (true) {
            if (slotSequenceCount < 0) {
                throw new IllegalStateException("Slot count wrapped around to negative");
            }

            String name = nameForSequenceNo(slotSequenceCount++);
            File f = new File(name);
            File parentDir = f.getParentFile();

            // Make sure directory exists
            if (! parentDir.exists()) {
                parentDir.mkdirs();
            }

            // If compressed version of slot file exists, skip that sequence number
            if (compressedSlotExists(name)) {
                continue;
            }

            // If file does not exist we have a winner
            if (! f.exists()) {
                return f;
            }

            // File exists.  Check if we are under the resume limit
            if (f.length() < resumeLimit) {
                return f;
            }

            // File exists but is over resume limit.  Go around again.
        }
    }

    /**
     * Write LogEvent to slot file.
     */
    public void write(Timber.LogEvent event) throws IOException {
        // Ensure that we have a RecordWriter
        if (null == currentWriter) {

            // Make sure we have not closed this Slot
            if (closed) {
                throw new IllegalStateException("Slot was closed");
            }

            currentFile = findNextSlotFile();
            if (null == currentFile) {
                // TODO(borud) create an exception for this
                throw new RuntimeException("Was unable to get next slot file");
            }

            // Pick up number of bytes in file
            numBytesInFile = currentFile.length();

            // Note: the FileOutPutStream must have append = true
            currentWriter = new RecordWriter(
                new BufferedOutputStream(
                    new FileOutputStream(currentFile, true),
                    outputBufferSize));
            writeCount = 0;
        }

        // Invariant: we have a currentWriter
        numBytesInFile += currentWriter.write(event);
        writeCount++;

        // Check if it is time to finish this file
        if (numBytesInFile > maxSize) {
            closeInternal();
        }
    }

    /**
     * Get the number of bytes written to the current slot file.
     *
     * @return number of bytes written to current slot file so far.
     */
    public long getNumBytesInFile() {
        return numBytesInFile;
    }

    /**
     * Close the current writer and ditch the currentWriter and
     * currentFile.  Used only internally.
     */
    private void closeInternal() throws IOException {
        if (null == currentWriter) {
            return;
        }
        currentWriter.close();
        currentWriter = null;
        currentFile = null;
    }

    /**
     * This method should only be used by unit tests.
     */
    public String getCurrentSlotFileName() {
        if (null == currentFile) {
            return null;
        }

        return currentFile.getAbsolutePath();
    }

    /**
     * Flush output to file.
     */
    public void flush()
        throws IOException
    {
        if (null == currentWriter) {
            return;
        }
        currentWriter.flush();
    }


    /**
     * Close this slot.  A Slot that has been closed must be
     * discarded.  You cannot write() to a closed slot.
     */
    public void close() throws IOException {
        closeInternal();
        closed = true;
    }

    public String toString() {
        return "prefix=" + prefix
            + ", maxSize=" + maxSize
            + ", resumeLimit=" + resumeLimit
            + ", outputBufferSize=" + outputBufferSize
            + ", slotSequenceCount=" + slotSequenceCount
            + ", currentFile=" + ((null == currentFile) ? "none" : currentFile.getAbsolutePath())
            + ", writeCount=" + writeCount
            + ", numBytesInFile=" + numBytesInFile
            ;

    }
}