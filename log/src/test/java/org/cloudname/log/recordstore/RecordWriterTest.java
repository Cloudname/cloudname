package org.cloudname.log.recordstore;

import org.cloudname.log.pb.Timber;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit test for RecordWriter class.
 *
 * @author borud
 */
public class RecordWriterTest {

    /**
     * Utility method for creating a log message.
     * @return a Timber.LogEvent with a given text message.
     */
    public static Timber.LogEvent createMessage(String message) {
        return Timber.LogEvent.newBuilder()
            .setTimestamp(1000000)
            .setLevel(1)
            .setHost("example.com")
            .setServiceName("myservice")
            .setSource(RecordWriterTest.class.getName())
            .setPid(0)
            .setTid((int) Thread.currentThread().getId())
            .setType("T")
            .addPayload(
                Timber.Payload.newBuilder()
                .setName("msg")
                .setPayload(ByteString.copyFromUtf8(message)))
            .build();
    }

    /**
     * Perform simple write test.
     */
    @Test public void writeTest() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RecordWriter writer = new RecordWriter(out);

        // Generate some log messages and write them
        int numMessages = 10;
        for (int i = 0; i < numMessages; i++) {
            int n = writer.write(createMessage("This is log message " + i));
            assertTrue(n > 0);
        }

        byte[] data = out.toByteArray();

        ByteArrayInputStream inp = new ByteArrayInputStream(data);

        Timber.LogEvent ev = null;
        int count = 0;
        do {
            ev = Timber.LogEvent.parseDelimitedFrom(inp);
            if (null != ev) {
                count++;
                assertEquals("T", ev.getType());
            }
        } while (null != ev);

        assertEquals(numMessages, count);
    }
}