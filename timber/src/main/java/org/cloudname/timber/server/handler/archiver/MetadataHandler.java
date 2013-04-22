package org.cloudname.timber.server.handler.archiver;

import org.cloudname.log.archiver.*;
import org.cloudname.log.pb.Timber;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class will create a metadata entry for a LogEvent. Metadata entries are held in a
 * file with the same name as the Slot file, but with a {METADATA_FILE_SUFFIX}. A metadata entry
 * consists of the logevent's id, write count, start byte offset and end byte offset.
 *
 * Get the active MetadataHandler through the getInstance() method.
 * @author acidmoose
 */
public class MetadataHandler {
    private static final int MAX_FILES_OPEN = 5;

    private static final Logger LOG = Logger.getLogger(MetadataHandler.class.getName());

    public static final String DELIMITER = ",";
    public static final String METADATA_FILE_SUFFIX = "_md";

    public static MetadataHandler instance;

    private final Object lock = new Object();

    private File currentSlotFile;
    private BufferedWriter currentWriter;
    private final MetadataWriterLruCache<String, BufferedWriter> writerLruCache
        = new MetadataWriterLruCache<String, BufferedWriter>(MAX_FILES_OPEN);

    private MetadataHandler() {}

    /**
     * Get the instance of the MetadataHandler.
     * @return
     */
    public static MetadataHandler getInstance() {
        if (instance == null) {
            instance = new MetadataHandler();
        }
        return instance;
    }

    /**
     * Writes an acked event id to the metadata file.
     * @param slotFile the Slot file for the acked event.
     * @param id the id of the event acked.
     */
    public void writeAck(final File slotFile, final String id) {
        synchronized (lock) {
            try {
                final BufferedWriter writer = getWriter(slotFile);
                assert(writer != null);
                writer.write(id);
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Unable to write metadata ack entry.", e);
            }
        }
    }

    /**
     * Flushes all metadata writers.
     * @throws IOException
     */
    public void flush() throws IOException {
        if (currentWriter != null) {
            currentWriter.flush();
        }
        for (final BufferedWriter writer : writerLruCache.values()) {
            try {
                writer.flush();
            } catch (IOException e) {
                throw new ArchiverException("Got IOException while flushing " + writer.toString(), e);
            }
        }
    }

    /**
     * Closes all metadata writers.
     * @throws IOException
     */
    public void close() throws IOException {
        flush();
        if (currentWriter != null) {
            currentWriter.close();
        }
        for (final BufferedWriter writer : writerLruCache.values()) {
            try {
                writer.close();
            } catch (IOException e) {
                throw new ArchiverException("Got IOException while flushing " + writer.toString(), e);
            }
        }
    }

    private BufferedWriter getWriter(final File slotFile) throws IOException {
        if (currentSlotFile == null || !currentSlotFile.equals(slotFile)) {
            final BufferedWriter writer = writerLruCache.get(slotFile.getAbsolutePath() + METADATA_FILE_SUFFIX);
            if (writer != null) {
                return writer;
            }
            final File mdFile = new File(slotFile.getAbsolutePath() + METADATA_FILE_SUFFIX);
            if (!mdFile.exists()) {
                if (! mdFile.getParentFile().exists()) {
                    mdFile.getParentFile().mkdirs();
                }
                mdFile.createNewFile();
            }
            currentSlotFile = slotFile;
            currentWriter = new BufferedWriter(new FileWriter(mdFile, true));
            writerLruCache.put(mdFile.getAbsolutePath(), currentWriter);
        }
        return currentWriter;
    }
}
