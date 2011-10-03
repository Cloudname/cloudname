package org.cloudname.log.archiver;

import org.cloudname.log.pb.Timber;

import java.io.File;
import java.io.IOException;

/**
 * This class implements a very simplistic log archiver that will
 * archive logs using the Timber format to archive log messages in
 * archive slots.
 *
 * <i> This implementation was originally written for the Timber log
 * server but was moved here since we want to use the same code in
 * multiple different settings.  This is why this log archiver has
 * some machinery for managing multiple open slots; in a server
 * setting we have to assume that some portion of the log messages
 * will be delayed.  This code will be revisited and optimized if it
 * is deemed necessary.</i>
 *
 * @author borud
 */
public class Archiver {
    private static final int MAX_FILES_OPEN = 5;

    private final SlotMapper slotMapper = new SlotMapper();
    private final SlotLruCache<String,Slot> slotLruCache = new SlotLruCache<String,Slot>(MAX_FILES_OPEN);

    private String logPath;
    private File logDir;
    private long maxFileSize;

    private boolean closed = false;

    /**
     * The directory
     */
    public Archiver(String logPath, long maxFileSize) {
        this.logPath = logPath;
        this.maxFileSize = maxFileSize;
    }

    /**
     * Initialize the archiver.
     */
    public void init() {
        logDir = new File(logPath);

        // Make the root log directory if it does not exist
        if (! logDir.exists()) {
            logDir.mkdirs();
        }
    }

    public void handle(Timber.LogEvent logEvent) {
        if (closed) {
            throw new IllegalStateException("Archiver was closed");
        }

        try {
            getSlot(logEvent).write(logEvent);
        } catch (IOException e) {
            throw new ArchiverException("Got IOException while handling logEvent", e);
        }
    }

    public void flush() {
        for (Slot slot : slotLruCache.values()) {
            try {
                slot.flush();
            } catch (IOException e) {
                throw new ArchiverException("Got IOException while flushing " + slot.toString(), e);
            }
        }
    }

    public void close() {
        for (Slot slot : slotLruCache.values()) {
            try {
                slot.close();
            } catch (IOException e) {
                throw new ArchiverException("Got IOException while closing " + slot.toString(), e);
            }
        }
        closed = true;
    }

    public String getName() {
        return Archiver.class.getName();
    }

    /**
     * @return the slot a Timber.LogEvent belongs in.
     */
    private Slot getSlot(Timber.LogEvent event) {
        String slotPathPrefix = logPath + File.separator + slotMapper.map(event.getTimestamp());
        Slot slot = slotLruCache.get(slotPathPrefix);
        if (null != slot) {
            return slot;
        }

        slot = new Slot(slotPathPrefix, maxFileSize);
        slotLruCache.put(slotPathPrefix, slot);

        return slot;
    }
}