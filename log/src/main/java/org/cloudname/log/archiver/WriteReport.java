package org.cloudname.log.archiver;

import java.io.File;

/**
 * Report for a write operation, used by Archiver.
 * @author acidmoose
 */
public class WriteReport {
    private File slotFile;
    private long startOffset;
    private long endOffset;
    private int writeCount;

    /**
     * Set the slot file written to.
     * @param slotFile written to
     */
    public void setSlotFile(final File slotFile) {
        this.slotFile = slotFile;
    }

    /**
     * Set the start byte offset for the write operation.
     * @param startOffset start byte offset
     */
    public void setStartOffset(final long startOffset) {
        this.startOffset = startOffset;
    }

    /**
     * Set the end byte offset for the write operation.
     * @param endOffset end byte offset
     */
    public void setEndOffset(final long endOffset) {
        this.endOffset = endOffset;
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
     * Set the current number of elements in the slot file.
     * @param writeCount number of elements in slot file
     */
    public void setWriteCount(final int writeCount) {
        this.writeCount = writeCount;
    }

    /**
     * Get the current number of elements in the slot file.
     * @return number of elements in slot file
     */
    public int getWriteCount() {
        return writeCount;
    }
}
