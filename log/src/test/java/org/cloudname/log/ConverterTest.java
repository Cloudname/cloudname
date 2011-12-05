package org.cloudname.log;

import org.cloudname.log.pb.Timber;

import java.util.logging.LogRecord;
import java.util.logging.Level;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for Convert class.
 *
 * @author borud
 */
public class ConverterTest {
    private static final String fakeServiceName = "myservice";
    private static final String fakeClassName = "org.cloudname.log.SomeClass";
    private static final String fakeMethodName = "theMethod";
    private static final String logMessage = "this is the logmessage";
    private static LogRecord rec;

    /**
     * Create a LogRecord instance that we can use in this test.
     */
    @BeforeClass
    public static void setup() {
        rec = new LogRecord(Level.INFO, logMessage);
        rec.setSourceClassName(fakeClassName);
        rec.setLoggerName(fakeClassName);
        rec.setSourceMethodName(fakeMethodName);
        rec.setLevel(Level.INFO);
    }

    /**
     * Create a converter and try to convert a
     * java.util.logging.LogRecord to Timber.LogEvent.  Ensure that
     * the results are what we expect.
     */
    @Test
    public void testSimple() {
        Converter conv = new Converter(fakeServiceName);
        Timber.LogEvent event = conv.convertFrom(rec);

        assertEquals(rec.getLevel().intValue(), event.getLevel());
        assertEquals(rec.getMillis(), event.getTimestamp());
        assertEquals(rec.getThreadID(), event.getTid());
        assertEquals(rec.getSourceClassName() + "#" + rec.getSourceMethodName(),
                     event.getSource());

        // The fields for which we have no value or constant values
        assertEquals(0, event.getPid());
        assertEquals(fakeServiceName, event.getServiceName());
        assertEquals("T", event.getType());

        // Make sure there is a single payload and that it contains
        // the raw message
        assertEquals(1, event.getPayloadCount());

        Timber.Payload payload = event.getPayload(0);
        assertEquals("msg", payload.getName());
        assertEquals(logMessage, payload.getPayload().toStringUtf8());
    }

    @Test
    public void testException() {
        Converter conv = new Converter(fakeServiceName);
        LogRecord rec = new LogRecord(Level.INFO, "bleh");
        rec.setThrown(new RuntimeException("meh"));
        Timber.LogEvent event = conv.convertFrom(rec);

        assertEquals("msg", event.getPayload(0).getName());
        assertEquals("application/java-exception", event.getPayload(1).getContentType());
    }
}
