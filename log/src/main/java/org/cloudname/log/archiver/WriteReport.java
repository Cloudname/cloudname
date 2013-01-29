package org.cloudname.log.archiver;

import java.io.File;

/**
 * Report for a write operation, used by Archiver.
 * @author acidmoose
 */
public class WriteReport {
    private final File slotFile;
    private final long startOffset;
    private final long endOffset;
    private final int writeCount;

    /**
     * Construct a WriteReport
     * @param slotFile the slot file written to.
     * @param startOffset the start byte offset for the write operation.
     * @param endOffset the end byte offset for the write operation.
     * @param writeCount the current number of elements in the slot file.
     */
    public WriteReport(final File slotFile, final long startOffset, final long endOffset, final int writeCount) {
        this.slotFile = slotFile;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.writeCount = writeCount;
    }

    /**
     * Get the slot file written to.
     * @return slot file written to
     */
    public File getSlotFile() {
        return slotFile;
    }

    /**
     * Get the start byte offset for the write operation.
     * @return start byte offset
     */
    public long getStartOffset() {
        return startOffset;
    }

    /**
     * Get the end byte offset for the write operation.
     * @return end byte offset
     */
    public long getEndOffset() {
        return endOffset;
    }

    /**
     * Get the current number of elements in the slot file.
     * @return number of elements in slot file
     */
    public int getWriteCount() {
        return writeCount;
    }
}
