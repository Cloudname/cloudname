package org.cloudname.timber.server.handler.archiver;

import com.google.protobuf.ByteString;
import junit.framework.Assert;
import com.telenor.sw.idgen.IdGenerator;
import org.cloudname.log.LogUtil;
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
        final IdGenerator.Builder builder = new IdGenerator.Builder();
        idGenerator = builder
            .setName("metadatahandlertest")
            .setWorkerId(1L).build();
    }

    /**
     * Write a metadata entry for a simple LogEvent.
     * @throws IOException
     */
    @Test
    public void testSimpleWrite() throws IOException {
        final File mdFolder = temp.newFolder("simpleWriteFolder");
        final File slotFile = new File(mdFolder.getAbsolutePath() + "/slotfile");
        slotFile.createNewFile();

        final String id = "id";
        final MetadataHandler handler = MetadataHandler.getInstance();

        final Timber.LogEvent event = Timber.LogEvent.newBuilder()
            .setTimestamp(System.currentTimeMillis())
            .setConsistencyLevel(Timber.ConsistencyLevel.BESTEFFORT)
            .setLevel(100)
            .setId(id)
            .setHost("testHost")
            .setServiceName("testService")
            .setSource("testSource")
            .setPid(0)
            .setTid((int) Thread.currentThread().getId())
            .setType("T")
            .addPayload(
                Timber.Payload.newBuilder()
                    .setName("msg")
                    .setPayload(ByteString.copyFromUtf8("text")))
            .build();

        final WriteReport wr = new WriteReport(slotFile, 2, 3, 1);

        handler.write(event, wr);
        handler.flush();

        final File mdFile = new File(
            slotFile.getAbsolutePath() + MetadataHandler.METADATA_FILE_SUFFIX);

        Assert.assertTrue(slotFile.exists());

        final BufferedReader reader = new BufferedReader(new FileReader(mdFile));

        final String line = reader.readLine();
        reader.close();

        Assert.assertEquals(
            id +
                MetadataHandler.DELIMITER +
                1 +
                MetadataHandler.DELIMITER +
                2 +
                MetadataHandler.DELIMITER +
                3,
            line);
    }

    /**
     * Test that multiple threads can use the MetadataHandler without problems.
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public void testMultipleThreadWrites() throws InterruptedException, IOException {
        final int numExecutingThreads = 10;
        final int numWriteRequestsPerThread = 10;

        final CountDownLatch startLatch = new CountDownLatch(numExecutingThreads);
        final CountDownLatch finishLatch
            = new CountDownLatch(numExecutingThreads * numExecutingThreads);

        final long startTimeoutMs = 10000L;
        final long endTimeout = startTimeoutMs + 10000L;

        final MetadataHandler handler = MetadataHandler.getInstance();

        final File mdFolder = temp.newFolder("multipleThreadFolder");
        final File slotFile = new File(mdFolder.getAbsolutePath() + "/slotfile");
        slotFile.createNewFile();
        final File slotMdFile =
            new File(slotFile.getAbsolutePath() + MetadataHandler.METADATA_FILE_SUFFIX);
        slotMdFile.createNewFile();

        final DateTime startDate = new DateTime("2010-12-31T23:50:00.000+01:00");

        for (int i = 0; i < numExecutingThreads; i++) {
            final Thread thread = new Thread(){
                public void run() {

                    // Wait for other threads to start.
                    startLatch.countDown();
                    try {
                        startLatch.await(startTimeoutMs, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        // Error situation. Throw exception and crash the test.
                        throw new RuntimeException(e);
                    }

                    for (int j = 0; j < numWriteRequestsPerThread; j++) {
                        final Timber.LogEvent event =
                            generateEvent(idGenerator, startDate.toDate().getTime() + j);
                        final WriteReport wr = new WriteReport(slotFile, j, j, j);
                        handler.write(event, wr);
                        finishLatch.countDown();
                    }
                }
            };
            thread.start();
        }

        Assert.assertEquals("", true, finishLatch.await(endTimeout, TimeUnit.MILLISECONDS));

        handler.flush();

        final BufferedReader reader = new BufferedReader(
            new FileReader(slotFile.getAbsolutePath() + MetadataHandler.METADATA_FILE_SUFFIX));
        int counter = 0;
        while (reader.readLine() != null) {
            counter++;
        }

        reader.close();

        Assert.assertEquals(numExecutingThreads * numWriteRequestsPerThread, counter);
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

        final BufferedReader reader = new BufferedReader(
            new FileReader(file.getAbsolutePath() + MetadataHandler.METADATA_FILE_SUFFIX));

        Assert.assertEquals("File content is not correct.", "ack,id", reader.readLine());

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
            handler.write(events.get(i), writeReports.get(i));
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
            handler.write(events.get(i), writeReports.get(i));
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
