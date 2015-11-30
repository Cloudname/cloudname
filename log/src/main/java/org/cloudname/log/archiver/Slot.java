package org.cloudname.log.archiver;

import org.cloudname.idgen.TimeProvider;
import org.cloudname.log.pb.Timber;
import org.cloudname.log.recordstore.RecordWriter;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

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
    private TimeProvider timeProvider;
    private final long resumeLimit;
    private final int outputBufferSize = DEFAULT_OUTPUT_BUFFER_SIZE;

    private File currentFile = null;
    private RecordWriter currentWriter = null;
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
    public Slot(final String prefix, final long maxSize) {
        this(prefix, maxSize, new TimeProvider() {
            @Override
            public long getTimeInMillis() {
                return System.currentTimeMillis();
            }
        });
    }

    /**
     * Create a Slot.
     *
     * @param prefix the prefix for the slot file
     * @param maxSize the maximum allowed file size in bytes for individual slot files
     * @param timeProvider optional custom TimeProvider
     */
    public Slot(final String prefix, final long maxSize, final TimeProvider timeProvider) {
        this.prefix = prefix;
        this.maxSize = maxSize;
        this.timeProvider = timeProvider;

        resumeLimit = (maxSize * RESUME_LIMIT_PERCENT) / 100;
    }

    /**
     * @return the slot file path for a given timestamp.
     */
    private String nameForTimestamp(final long timestamp) {
        return prefix + "_" + timestamp;
    }

    /**
     * Check if the prospective filename exists with a suffix
     * indicating it has been compressed.
     *
     * @param file the file for which we want to check if there is
     *   a compressed version.
     * @return {@code true} if a compressed file of this slot exists,
     *   {@code false} otherwise.
     */
    private static boolean compressedSlotExists(final File file) {
        return new File(file.getAbsolutePath() + ".gz").exists()
            || new File(file.getAbsolutePath() + ".bz2").exists();

    }

    /**
     * Find the next slot file to write to.
     *
     * @return a File instance that points to the next slot file
     */
    private File findNextSlotFile() throws IOException {
        final File file = new File(nameForTimestamp(timeProvider.getTimeInMillis()));
        final File parentDir = file.getParentFile();

        // Make sure directory exists
        if (!parentDir.exists()) {
            if (!parentDir.mkdirs()) {
                throw new ArchiverException("Could not create folder '"
                        + parentDir.getAbsolutePath() + "', hence slot-file logging will fail. " +
                        "Does your user have sufficient permissions to create folders?");
            }
        }

        File foundFile = null;
        long highestTimestamp = 0L;

        // Iterate over existing files to find a file to resume writing to.
        for (final File f : parentDir.listFiles()) {

            // Make sure it is a proper slot file
            if (! f.getAbsolutePath().contains(prefix)) {
                continue;
            }

            final String[] split = f.getName().split("_");
            final String lastSplit = split[split.length - 1];

            final long timestamp;
            try{
                timestamp = Long.parseLong(lastSplit);
            } catch (NumberFormatException e) {
                // File does not end in a number. Skip it.
                continue;
            }

            // Check if we have already found a newer file.
            if (timestamp < highestTimestamp) {
                continue;
            }

            // Check if compressed version exists.
            if (compressedSlotExists(f)) {
                continue;
            }

            // Check if we are allowed to resume writing to the file?
            if (f.length() > resumeLimit) {
                continue;
            }

            // Found new slot file with higher timestamp.
            highestTimestamp = timestamp;
            foundFile = f;
        }

        // No files with timestamp found. Return new slot file.
        if (foundFile == null) {
            return file;
        }

        // Resumable file found. Return it.
        return foundFile;
    }

    /**
     * Write LogEvent to slot file.
     */
    public WriteReport write(final Timber.LogEvent event) throws IOException {
        // Ensure that we have a RecordWriter
        if (null == currentWriter) {

            // Make sure we have not closed this Slot
            if (closed) {
                throw new IllegalStateException("Slot was closed");
            }

            currentFile = findNextSlotFile();

            // Pick up number of bytes in file
            numBytesInFile = currentFile.length();

            // Note: the FileOutPutStream must have append = true
            currentWriter = new RecordWriter(
                new BufferedOutputStream(
                    new FileOutputStream(currentFile, true),
                    outputBufferSize));
            writeCount = 0;
        }

        final long startOffset = numBytesInFile;

        // Invariant: we have a currentWriter
        numBytesInFile += currentWriter.write(event);
        writeCount++;

        // Make return value here to keep currentFile object (removed in closeIntenal()).
        final WriteReport ret =
            new WriteReport(currentFile, startOffset, numBytesInFile, writeCount);

        // Check if it is time to finish this file
        if (numBytesInFile > maxSize) {
            closeInternal();
        }

        return ret;
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
            + ", currentFile=" + ((null == currentFile) ? "none" : currentFile.getAbsolutePath())
            + ", writeCount=" + writeCount
            + ", numBytesInFile=" + numBytesInFile
            ;

    }
}
