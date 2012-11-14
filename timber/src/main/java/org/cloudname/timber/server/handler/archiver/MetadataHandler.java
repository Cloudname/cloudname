package org.cloudname.timber.server.handler.archiver;

import org.cloudname.log.archiver.WriteReport;
import org.cloudname.log.pb.Timber;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

    private static final Logger LOG = Logger.getLogger(MetadataHandler.class.getName());
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    public static final String DELIMITER = ",";
    public static final String METADATA_FILE_SUFFIX = ".md";

    public static MetadataHandler instance;

    private final Object lock = new Object();

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
     * Write a metadata entry for a LogEvent.
     * @param logEvent the logevent to create a metadata entry for
     * @param wr the write report from the Archiver
     */
    public void write(final Timber.LogEvent logEvent, final WriteReport wr) {
        synchronized (lock) {
            final File metaDataFile;
            try {
                metaDataFile = getMetadataFile(wr.getSlotFile());
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Unable to get metadata file.", e);
                return;
            }

            final BufferedWriter writer;
            try {
                writer = new BufferedWriter(new FileWriter(metaDataFile, true));
                writer.write(
                    logEvent.getId()
                        + DELIMITER
                        + wr.getWriteCount()
                        + DELIMITER
                        + wr.getStartOffset()
                        + DELIMITER
                        + wr.getEndOffset()
                        + LINE_SEPARATOR);
                writer.close();
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Unable to write to metadata file.", e);
                return;
            }
        }
    }

    /**
     * Writes an acked event id to the metadata file. Line of text looks like this (for event
     * with id "1"):
     * "ack,1"
     * @param slotFile the Slot file for the acked event.
     * @param id the id of the event acked.
     */
    public void writeAck(final File slotFile, final String id) {
        synchronized (lock) {
            final File metaDataFile;
            try {
                metaDataFile = getMetadataFile(slotFile);
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Unable to get metadata file.", e);
                return;
            }

            final BufferedWriter writer;
            try {
                writer = new BufferedWriter(new FileWriter(metaDataFile, true));
                writer.write("ack" + DELIMITER + id);
                writer.close();
            } catch (IOException e) {
                LOG.log(Level.WARNING, "Unable to write to metadata file.", e);
                return;
            }
        }
    }

    private File getMetadataFile(final File slotFile) throws IOException {
        final File mdFile = new File(slotFile.getAbsolutePath() + METADATA_FILE_SUFFIX);
        if (!mdFile.exists()) {
            mdFile.createNewFile();
        }
        return mdFile;
    }
}
