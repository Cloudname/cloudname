package org.cloudname.log.format;

import org.cloudname.log.pb.Timber;

import com.google.protobuf.ByteString;

import org.junit.*;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Unit tests for CompactFormatter.
 *
 * @author borud
 */
public class CompactFormatterTest {
    private static final Logger log = Logger.getLogger(CompactFormatterTest.class.getName());

    private static final String eventString
        = "2011-11-28T16:46:22.123\texample.com\tmyservice\tSingleLineFormatter\tT\tINFO\tBE\tmsg: this is a test";

    private static Timber.LogEvent event;
    private static Timber.LogEvent eventWithException;

    private static long instant = 1322498782123L;

    @BeforeClass
    public static void setUp() {
        event = Timber.LogEvent.newBuilder()
            .setTimestamp(instant)
            .setConsistencyLevel(Timber.ConsistencyLevel.BESTEFFORT)
            .setLevel(Level.INFO.intValue())
            .setHost("example.com")
            .setServiceName("myservice")
            .setSource(SingleLineFormatter.class.getName())
            .setPid(0)
            .setTid((int) Thread.currentThread().getId())
            .setType("T")
            .addPayload(
                Timber.Payload.newBuilder()
                .setName("msg")
                .setPayload(ByteString.copyFromUtf8("this is a test")))
            .build();

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        new RuntimeException("Testing").printStackTrace(new PrintStream(os));

        eventWithException = Timber.LogEvent.newBuilder()
            .setTimestamp(instant)
            .setConsistencyLevel(Timber.ConsistencyLevel.BESTEFFORT)
            .setLevel(Level.WARNING.intValue())
            .setHost("example.com")
            .setServiceName("myservice")
            .setSource(SingleLineFormatter.class.getName())
            .setPid(0)
            .setTid((int) Thread.currentThread().getId())
            .setType("T")
            .addPayload(
                Timber.Payload.newBuilder()
                .setName("msg")
                .setPayload(ByteString.copyFromUtf8("this is a test with an exception")))
            .addPayload(
                Timber.Payload.newBuilder()
                .setName("exception")
                .setContentType("application/java-exception")
                .setPayload(ByteString.copyFrom(os.toByteArray())))
            .build();
    }

    @Test
    public void simpleTest() throws Exception {
        CompactFormatter form = new CompactFormatter();
        assertEquals(eventString, form.format(event));
    }

    /**
     * A micro benchmark.
     */
    @Test (timeout = 1000)
    public void microBenchmarkTest() {
        CompactFormatter form = new CompactFormatter();
        int numIterations = 1000;

        long start = System.currentTimeMillis();
        for (int i = 0; i < numIterations; i++) {
            form.format(event);
        }
        long duration = System.currentTimeMillis() - start;
        double formatsPerSecond = numIterations / ((double) duration / 1000.0);

        log.info("event formats per second: " + formatsPerSecond
                 + " (" + numIterations + " iterations took " + duration + " milliseconds)");
    }

    @Test (timeout = 1000)
    public void microBenchmarkWithExceptionTest() {
        CompactFormatter form = new CompactFormatter();
        int numIterations = 1000;

        long start = System.currentTimeMillis();
        for (int i = 0; i < numIterations; i++) {
            form.format(eventWithException);
        }
        long duration = System.currentTimeMillis() - start;
        double formatsPerSecond = numIterations / ((double) duration / 1000.0);

        log.info("event + exception formats per second: " + formatsPerSecond
                 + " (" + numIterations + " iterations took " + duration + " milliseconds)");
    }
}