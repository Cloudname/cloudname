package org.cloudname.log;

import org.cloudname.log.pb.Timber;
import org.cloudname.log.archiver.SlotMapper;
import org.cloudname.log.recordstore.RecordReader;

import java.util.logging.Level;
import java.util.logging.LogRecord;

import org.junit.*;
import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;

/**
 * Unit test for ComboHandler.
 *
 * @author borud
 */
public class ComboHandlerTest {
    @Rule public TemporaryFolder temp = new TemporaryFolder();

    // To map timestamps to slots we can re-use the SlotMapper from
    // the log server code.
    SlotMapper slotMapper = new SlotMapper();

    @Test
    public void testSimple() throws Exception {
        File logDir = temp.newFolder("log");
        ComboHandler ch = new ComboHandler("-", logDir.getAbsolutePath());

        for (int i = 0; i < 3; i++) {
            LogRecord r = new LogRecord(Level.FINE, "This is number " + i);
            r.setSourceClassName(ComboHandlerTest.class.getName());
            r.setSourceMethodName("someMethod");
            r.setLoggerName("some.logger.name");
            // Trigger source fields to get set.
            ch.publish(r);
        }

        ch.flush();

        // This will fail if the test is run precisely at midnight UTC
        // and we are unlucky with the timing.  Just saying. :-)
        String slot = slotMapper.map(System.currentTimeMillis());
        String logFileName = logDir.getAbsolutePath() + "/" + slot + "_0";
        RecordReader rr = new RecordReader(new FileInputStream(logFileName));
        Timber.LogEvent event = null;
        try {
            while ((event = rr.read()) != null) {
                assertTrue(event.getPayload(0).getPayload().toStringUtf8().startsWith("This is number "));
            }
        } finally {
            rr.close();
        }
    }

    @Test
    public void testException() throws Exception {
        File logDir = temp.newFolder("log");
        ComboHandler ch = new ComboHandler("-", logDir.getAbsolutePath());

        // Create a log record with a nested exception in it.
        LogRecord r = new LogRecord(Level.FINE, "message with exception");
        r.setSourceClassName(ComboHandlerTest.class.getName());
        r.setSourceMethodName("someMethod");
        r.setLoggerName("some.logger.name");
        r.setThrown(new RuntimeException(new RuntimeException("sample exception")));
        ch.publish(r);
        ch.flush();

        String slot = slotMapper.map(System.currentTimeMillis());
        String logFileName = logDir.getAbsolutePath() + "/" + slot + "_0";
        RecordReader rr = new RecordReader(new FileInputStream(logFileName));
        Timber.LogEvent event = rr.read();

        // Make sure the message is there
        assertEquals("message with exception", event.getPayload(0).getPayload().toStringUtf8());

        // And the exception
        assertEquals("application/java-exception", event.getPayload(1).getContentType());
    }
}