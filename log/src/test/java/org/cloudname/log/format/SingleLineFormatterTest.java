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
 * Unit test for SingleLineFormatter.
 *
 * @author borud
 */
public class SingleLineFormatterTest {
    private static final Logger log = Logger.getLogger(SingleLineFormatterTest.class.getName());

    private static final String eventString
        = "1000.000\texample.com\t0/1\tmyservice\torg.cloudname.log.format.SingleLineFormatter\tT\tINFO\tBESTEFFORT\tmsg: this is a test";

    private static Timber.LogEvent event;
    private static Timber.LogEvent eventWithException;

    @BeforeClass
    public static void setUp() {
        event = Timber.LogEvent.newBuilder()
            .setTimestamp(1000000)
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
            .setTimestamp(1000000)
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

    /**
     * Just a straight forward conversion test.
     */
    @Test
    public void simpleTest() throws Exception {
        LogEventFormatter form = new SingleLineFormatter();
        assertEquals(eventString, form.format(event));
        assertNotNull(form.format(eventWithException));
    }

    /**
     * A micro benchmark.
     *
     * OBSERVATIONS: the bottleneck is the escape() method, however,
     * on runs with larger number of iterations the speed increases
     * dramatically, so it would appear that the hotspot kicks in and
     * optimizes the code path.  If the formatter needs to gain
     * significant speed it would pay off to replace the regexp
     * machinery with something that just iterates over the characters
     * and replaces newline with space, tab with space etc.
     *
     * (The bottleneck was observed by increasing numIterations to a
     * large number and profiling the test under jvisualvm)
     */
    @Test (timeout = 1000)
    public void microBenchmarkTest() {
        int numIterations = 1000;

        long start = System.currentTimeMillis();
        for (int i = 0; i < numIterations; i++) {
            LogEventFormatter form = new SingleLineFormatter();
            form.format(event);
        }
        long duration = System.currentTimeMillis() - start;
        double formatsPerSecond = numIterations / ((double) duration / 1000.0);

        log.info("event formats per second: " + formatsPerSecond
                 + " (" + numIterations + " iterations took " + duration + " milliseconds)");
    }

    /**
     * OBSERVATIONS: with multiple payloads it seems the formatter
     * performs atrociously bad.  Again the escape() method is to
     * blame.
     */
    @Test (timeout = 1000)
    public void microBenchmarkWithExceptionTest() {
        int numIterations = 1000;

        long start = System.currentTimeMillis();
        for (int i = 0; i < numIterations; i++) {
            LogEventFormatter form = new SingleLineFormatter();
            form.format(eventWithException);
        }
        long duration = System.currentTimeMillis() - start;
        double formatsPerSecond = numIterations / ((double) duration / 1000.0);

        log.info("event + exception formats per second: " + formatsPerSecond
                 + " (" + numIterations + " iterations took " + duration + " milliseconds)");
    }
}