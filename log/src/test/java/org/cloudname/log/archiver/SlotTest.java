package org.cloudname.log.archiver;

import org.cloudname.log.pb.Timber;
import static org.cloudname.log.pb.Timber.ConsistencyLevel;
import org.cloudname.log.recordstore.RecordReader;

import com.google.protobuf.ByteString;

import java.io.File;
import java.io.FileInputStream;

import java.util.logging.Logger;

import org.junit.*;
import static org.junit.Assert.*;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for Slot class.
 *
 * @author borud
 */
public class SlotTest {
    private static final Logger log = Logger.getLogger(SlotTest.class.getName());

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    // Just a point in time that we use in our tests
    private static final long pointInTime = 1321390697L;


    /**
     * Utility method to make a log event.
     * @param time the timestamp of the log event
     * @return a Timber.LogEvent instance
     */
    private Timber.LogEvent makeLogEvent(long time)
    {
        return Timber.LogEvent.newBuilder()
            .setTimestamp(time)
            .setConsistencyLevel(ConsistencyLevel.BESTEFFORT)
            .setLevel(1)
            .setHost("example.com")
            .setServiceName("myservice")
            .setSource(SlotTest.class.getName())
            .setPid(0)
            .setTid((int) Thread.currentThread().getId())
            .setType("T")
            .addPayload(
                Timber.Payload.newBuilder()
                .setName("msg")
                .setPayload(ByteString.copyFromUtf8("log message from t=" + time)))
            .build();
    }

    /**
     * Simple test to just make sure that things work without blowing up.
     */
    @Test
    public void simpleTest()
        throws Exception
    {
         String prefix = temp.newFolder("test1").getAbsolutePath();
         Slot slot = new Slot(prefix, (10 * 1024 * 1024));
         slot.write(makeLogEvent(0));
    }

    @Test
    public void writeLogMessages()
        throws Exception
    {
        // Prefix with extra directory level to trigger directory
        // creation code path in Slot
        String prefix = temp.newFolder("test2").getAbsolutePath()
            + File.separator + "slots"
            + File.separator + "slot"
            ;

        // Make slot with room for 50k
        long slotSize = 50 * 1024;
        Slot slot = new Slot(prefix, slotSize);

        // The number of log events has been carefully chosen by sheer
        // intellectual labor to provide perfect testing conditions.
        // On a more serious note, don't change this magic value
        // willy-nilly because there are certain tests below that
        // depend on not being on or near the size limit.
        Timber.LogEvent event = null;
        long t = System.currentTimeMillis();
        for (int i = 0; i < 2000; i++) {
            slot.write(makeLogEvent(t + i));
        }

        // Get the current slot filename.  When we resume we should
        // resume appending to the same file.  Make sure that the test
        // is tuned so that there is more room in the last slot file.
        String slotFileName = slot.getCurrentSlotFileName();

        // Close the slot and restart
        slot.close();

        // Write more.  Should resume
        t = System.currentTimeMillis();
        slot = new Slot(prefix, slotSize);

        // Make sure we start where we left off.
        slot.write(makeLogEvent(t));
        assertEquals(slotFileName, slot.getCurrentSlotFileName());

        // Now close it and rename the file to indicate it has been compressed
        slot.close();

        // Rename the file
        new File(slotFileName).renameTo(new File(slotFileName + ".gz"));

        slot = new Slot(prefix, slotSize);

        slot.write(makeLogEvent(t));
        assertFalse(slotFileName.equals(slot.getCurrentSlotFileName()));
    }

    @Test (expected = IllegalStateException.class)
    public void testClosedSlot()
        throws Exception
    {
         String prefix = temp.newFolder("test3").getAbsolutePath();
         Slot slot = new Slot(prefix, (10 * 1024 * 1024));
         slot.write(makeLogEvent(System.currentTimeMillis()));
         slot.close();
         slot.write(makeLogEvent(System.currentTimeMillis()));
    }

    /**
     * Make sure that When we resume writing to a partially filled
     * slot we do not overwrite the file and that indeed all the log
     * messages make it into the file.
     */
    @Test
    public void testResumedWriting() throws Exception
    {
        String prefix = temp.newFolder("test5-resume").getAbsolutePath();
        Slot slot = new Slot(prefix, (100 * 1024 * 1024));

        for (int i = 0; i < 10; i++) {
            slot.write(makeLogEvent(i));
        }

        String slotFileName = slot.getCurrentSlotFileName();

        slot.close();

        slot = new Slot(prefix, (100 * 1024 * 1024));
        slot.write(makeLogEvent(2000));

        assertEquals(slotFileName, slot.getCurrentSlotFileName());
        slot.close();

        RecordReader reader = new RecordReader(new FileInputStream(slotFileName));

        // The first 10 entries should have timestamp equal to i
        for (long i = 0; i < 10; i++) {
            Timber.LogEvent event = reader.read();
            assertEquals(i, event.getTimestamp());
        }

        // Now ensure we got the odd one out
        assertEquals(2000l, reader.read().getTimestamp());

        // ...and that the next read returns null
        assertNull(reader.read());
    }


    /**
     * A microbenchmark to ensure that the performance isn't crap.
     *
     * My workstation runs this test in about 300ms so we have added
     * 10x headroom since the CI server is really, really slow.  This
     * should trigger a breakage if the performance worsens by an
     * order of magnitude.
     */
    @Test (timeout = 3000)
    public void microBenchmark()
        throws Exception
    {
        String prefix = temp.newFolder("test4-benchmark").getAbsolutePath();
        Slot slot = new Slot(prefix, (100 * 1024 * 1024));

        Timber.LogEvent event = makeLogEvent(0);
        int numMessages = 100000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < numMessages; i++) {
            slot.write(event);
        }
        long duration = System.currentTimeMillis() - start;
        long rate = (numMessages * 1000) / duration;

        log.info("Slot microbenchmark: "+ numMessages + " in " + duration + " ms,"
                 + " rate = " + rate + " msg/sec");
    }

    /**
     * This test is an experimental unit test to figure out if we can
     * reliably determine the slot file size without stat()'ing the
     * file via the File.length() method. (Was used while fixing the
     * RecordWriter class, but we leave it in here to ensure that this
     * property will hold.
     */
    @Test
    public void testSlotFileSize() throws Exception {
        String prefix = temp.newFolder("test-slot-file-size").getAbsolutePath();
        Slot slot = new Slot(prefix, (20 * 1024 * 1024));

        for (int i = 0; i < 1000; i++) {
            Timber.LogEvent event = makeLogEvent(pointInTime + i);
            slot.write(event);

            if ((i % 100) == 0) {
                slot.flush();

                // Make sure the lengths are in sync
                long internalLength = slot.getNumBytesInFile();
                long fileLength = new File(slot.getCurrentSlotFileName()).length();
                assertEquals(fileLength, internalLength);
            }
        }

        // Close and reopen slot to ensure that the counts are correct
        // when write resumes.  This should re-open the same slot as
        // before (we verify this a bit further down).
        String filename = slot.getCurrentSlotFileName();
        slot.close();
        slot = new Slot(prefix, (20 * 1024 * 1024));

        // Now close the slot, re-open it and run again
        for (int i = 0; i < 1000; i++) {
            Timber.LogEvent event = makeLogEvent(pointInTime + 1000 + i);
            slot.write(event);

            if ((i % 100) == 0) {
                slot.flush();

                // Make sure the lengths are in sync
                long internalLength = slot.getNumBytesInFile();
                long fileLength = new File(slot.getCurrentSlotFileName()).length();
                assertEquals(fileLength, internalLength);
            }
        }

        // Make sure it was the same slot file as before.  We have to
        // do this here because the files are opened lazily.
        assertEquals(filename, slot.getCurrentSlotFileName());
        slot.close();

        // Now read the logevents back and make sure they check out
        RecordReader reader = new RecordReader(new FileInputStream(filename));
        for (int i = 0; i < 2000; i++) {
            Timber.LogEvent event = reader.read();
            assertEquals((pointInTime + i), event.getTimestamp());
        }

        // Ensure that we have reached EOF
        assertNull(reader.read());
        reader.close();
    }
}
