package org.cloudname.timber.server.handler.archiver;

import com.google.protobuf.ByteString;
import junit.framework.Assert;
import org.cloudname.log.LogUtil;
import org.cloudname.log.archiver.WriteReport;
import org.cloudname.log.pb.Timber;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test the MetadataHandler.
 * @author acidmoose
 */
public class MetadataHandlerTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Before
    public void setup() {

    }

    /**
     * Write a metadata entry for a simple LogEvent.
     * @throws IOException
     */
    @Test
    public void testSimpleWrite() throws IOException {
        final File mdFolder = temp.newFolder("simpleWriteFolder");
        final File slotFile = new File(mdFolder.getAbsolutePath() + "slotfile");
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

        final WriteReport wr = new WriteReport();
        wr.setSlotFile(slotFile);
        wr.setWriteCount(1);
        wr.setStartOffset(2);
        wr.setEndOffset(3);

        handler.write(event, wr);

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
        final File slotFile = new File(mdFolder.getAbsolutePath() + "slotfile");
        slotFile.createNewFile();

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
                        final Timber.LogEvent event = LogUtil.textEvent(10,
                            "myservice",
                            SimpleArchiverTest.class.getName(),
                            "some payload " + j);
                        final WriteReport wr = new WriteReport();
                        wr.setWriteCount(j);
                        wr.setEndOffset(j);
                        wr.setSlotFile(slotFile);
                        wr.setStartOffset(j);
                        handler.write(event, wr);
                        finishLatch.countDown();
                    }
                }
            };
            thread.start();
        }

        Assert.assertEquals("", true, finishLatch.await(endTimeout, TimeUnit.MILLISECONDS));

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
}
