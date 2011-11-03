package org.cloudname.log.recordstore;

import org.cloudname.log.pb.Timber;

import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit test for RecordReader.
 *
 * @author borud
 */
public class RecordReaderTest {

    /**
     * Test reading log records.
     */
    @Test (timeout=200)
    public void testReader() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RecordWriter writer = new RecordWriter(out);

        // Create a bunch of messages and write them to a buffer
        int numMessages = 100;
        for (int i = 0; i < numMessages; i++) {
            writer.write(RecordWriterTest.createMessage("Test record " + i));
        }

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        RecordReader reader = new RecordReader(in);

        int count = 0;
        Timber.LogEvent event = null;

        while ((event = reader.read()) != null) {
            count++;
            assertEquals(1, event.getPayloadCount());
            assertEquals("T", event.getType());
            String message = event.getPayload(0).getPayload().toStringUtf8();
            assertNotNull(message);
        }
        assertEquals(numMessages, count);

        reader.close();
    }
}