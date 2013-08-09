package org.cloudname.timber.server.handler.archiver;

import com.google.protobuf.ByteString;
import junit.framework.Assert;
import org.cloudname.idgen.IdGenerator;
import org.cloudname.log.archiver.WriteReport;
import org.cloudname.log.pb.Timber;
import org.joda.time.DateTime;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test the MetadataHandler.
 * @author acidmoose
 */
public class MetadataHandlerTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private IdGenerator idGenerator;

    @Before
    public void setup() {
        idGenerator = new IdGenerator(1L);
    }

    /**
     * Check the method for writing acks.
     * @throws IOException
     */
    @Test
    public void testWriteAck() throws IOException {
        final MetadataHandler handler = MetadataHandler.getInstance();
        final File file = temp.newFile("ackfile");

        handler.writeAck(file, "id");
        Assert.assertTrue(file.exists());
        final BufferedReader reader = new BufferedReader(
            new FileReader(file.getAbsolutePath() + MetadataHandler.METADATA_FILE_SUFFIX));
        String line = reader.readLine();
        Assert.assertEquals("File content is not correct.", "id", line);

        reader.close();
    }

    @Test
    @Ignore
    public void benchmarkBestCase() throws IOException {

        final File slotFile = temp.newFile();

        final MetadataHandler handler = MetadataHandler.getInstance();
        final int numEntries = 100000;

        final List<Timber.LogEvent> events = new ArrayList<Timber.LogEvent>();
        for (int i = 0; i < numEntries; i++) {
            events.add(i, generateEvent(idGenerator, i));
        }

        final List<WriteReport> writeReports = new ArrayList<WriteReport>();
        for (int i = 0; i < numEntries; i++) {
            writeReports.add(i, new WriteReport(slotFile, 0L, 0L, i));
        }

        final long start = System.currentTimeMillis();
        for (int i = 0; i < numEntries; i++) {
            handler.writeAck(writeReports.get(i).getSlotFile(), events.get(i).getId());
        }
        handler.flush();
        final long end = System.currentTimeMillis();

        System.out.println(numEntries + " entries written in " + (end-start) + " ms.");
    }

    @Test
    @Ignore
    public void benchmarkWorstCase() throws IOException {

        final File slotFile = temp.newFile();
        final File slotFile2 = temp.newFile();

        final MetadataHandler handler = MetadataHandler.getInstance();
        final int numEntries = 100000;

        final List<Timber.LogEvent> events = new ArrayList<Timber.LogEvent>();
        for (int i = 0; i < numEntries; i++) {
            events.add(i, generateEvent(idGenerator, i));
        }

        final List<WriteReport> writeReports = new ArrayList<WriteReport>();
        for (int i = 0; i < numEntries; i++) {
            writeReports.add(i, new WriteReport(((i % 2) == 0) ? slotFile : slotFile2, 0L, 0L, i));
        }

        final long start = System.currentTimeMillis();
        for (int i = 0; i < numEntries; i++) {
            handler.writeAck(writeReports.get(i).getSlotFile(), events.get(i).getId());
        }
        handler.flush();
        final long end = System.currentTimeMillis();

        System.out.println(numEntries + " entries written in " + (end-start) + " ms.");
    }

    private Timber.LogEvent generateEvent(final IdGenerator idGenerator, final long timestamp) {
        final Timber.LogEvent.Builder builder = Timber.LogEvent.newBuilder();
        builder
            .setTimestamp(timestamp)
            .setConsistencyLevel(Timber.ConsistencyLevel.SYNC)
            .setLevel(800)
            .setHost("host")
            .setServiceName("servicename")
            .setSource(MetadataHandlerTest.class.getName())
            .setPid(0)
            .setTid((int) Thread.currentThread().getId())
            .setType("T")
            .addPayload(
                Timber.Payload.newBuilder()
                    .setName("msg")
                    .setPayload(ByteString.copyFromUtf8("some payload")));
        if (idGenerator != null) {
            builder.setId(Long.toString(idGenerator.getNextId()));
        }
        return builder.build();
    }
}
