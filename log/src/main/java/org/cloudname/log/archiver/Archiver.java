package org.cloudname.log.archiver;

import org.cloudname.idgen.TimeProvider;
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
    private static final int MAX_FILES_OPEN = 2;

    private String service = "";

    private final SlotMapper slotMapper = new SlotMapper();
    private final SlotLruCache<String,Slot> slotLruCache = new SlotLruCache<String,Slot>(MAX_FILES_OPEN);

    private final String logPath;
    private final long maxFileSize;
    private TimeProvider timeProvider;

    private boolean closed = false;

    /**
     * The directory
     * @param logPath folder to store logs
     * @param service name of the service storing logs
     * @param maxFileSize maximum file size in bytes
     */
    public Archiver(String logPath, String service, long maxFileSize) {
        this(logPath, service, maxFileSize, new TimeProvider() {
            @Override
            public long getTimeInMillis() {
                return System.currentTimeMillis();
            }
        });
    }

    /**
     * The directory
     * @param logPath folder to store logs
     * @param service name of the service storing logs
     * @param maxFileSize maximum file size in bytes
     * @param timeProvider option custom TimeProvider
     */
    public Archiver(String logPath, String service, long maxFileSize, TimeProvider timeProvider) {
        this.logPath = logPath;
        this.service = service;
        this.maxFileSize = maxFileSize;
        this.timeProvider = timeProvider;
    }

    /**
     * Initialize the archiver.  If the logging directory specified in
     * the constructor does not exist it will be created.
     */
    public void init() {
        final File logDir = new File(logPath);

        // Make the root log directory if it does not exist
        if (! logDir.exists()) {
            logDir.mkdirs();
        }
    }

    /**
     * Append log event to the appropriate slot file given the
     * timestamp of the LogEvent.
     *
     * @param logEvent the LogEvent we wish to log.
     * @throws IllegalStateException if the archiver was closed.
     * @throws ArchiverException if an io error occurred when trying
     *   to write a log event.  The original IO exception causing the
     *   problem will be chained.
     * @return WriteReport containing information about the write operation.
     */
    public WriteReport handle(final Timber.LogEvent logEvent) {
        if (closed) {
            throw new IllegalStateException("Archiver was closed");
        }

        try {
            return getSlot(logEvent).write(logEvent);
        } catch (IOException e) {
            throw new ArchiverException("Got IOException while handling logEvent", e);
        }
    }

    /**
     * Ensure that all currently opened slot files are flushed to
     * disk.
     *
     * @throws ArchiverException if an io error occurred when trying
     *   to flush the slot files.  The original IO exception causing
     *   the problem will be chained.
     *
     */
    public void flush() {
        for (final Slot slot : slotLruCache.values()) {
            try {
                slot.flush();
            } catch (IOException e) {
                throw new ArchiverException("Got IOException while flushing " + slot.toString(), e);
            }
        }
    }

    /**
     * Close the archiver.  Closes all the currently open slot files.
     * After an Archiver has been closed it cannot be re-opened.  Any
     * attempt at logging messages to a closed Archiver will result in
     * an IllegalStateException.
     *
     * @throws ArchiverException if an io error occurred when trying
     *   to flush the slot files.  The original IO exception causing
     *   the problem will be chained.
     */
    public void close() {
        for (final Slot slot : slotLruCache.values()) {
            try {
                slot.close();
            } catch (IOException e) {
                throw new ArchiverException("Got IOException while closing " + slot.toString(), e);
            }
        }
        closed = true;
    }

    /**
     * @return the slot a Timber.LogEvent belongs in.
     */
    private Slot getSlot(Timber.LogEvent event) {
        String slotPathPrefix = logPath + File.separator + slotMapper.map(event.getTimestamp(), service);
        Slot slot = slotLruCache.get(slotPathPrefix);
        if (null != slot) {
            return slot;
        }

        slot = new Slot(slotPathPrefix, maxFileSize, timeProvider);
        slotLruCache.put(slotPathPrefix, slot);

        return slot;
    }
}
