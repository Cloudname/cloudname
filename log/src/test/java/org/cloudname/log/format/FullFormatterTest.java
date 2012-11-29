package org.cloudname.log.format;

import com.google.protobuf.ByteString;
import org.cloudname.log.pb.Timber;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Unit test for FullFormatter.
 *
 * @author storsveen
 */
public class FullFormatterTest {
    private static final Logger log = Logger.getLogger(CompactFormatterTest.class.getName());

    private static final String eventString
        = "2011-11-28T16:46:22.123\texample.com\t0/1\tmyservice\tSingleLineFormatter\tT\tINFO\tBE\tmsg: this is a test";

    private static final String eventStringWithException
        = "2011-11-28T16:46:22.123\texample.com\t0/1\tmyservice\tSingleLineFormatter\tT\tWARNING" +
        "\tBE\tmsg: this is a test with an exception | exception: java.lang.RuntimeException: " +
        "Testing\\n\\tat";

    private static Timber.LogEvent event;
    private static Timber.LogEvent eventWithException;

    private static long instant = 1322498782123L;
    private static RuntimeException runtimeException = new RuntimeException("Testing");

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
        runtimeException.printStackTrace(new PrintStream(os));

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

    /**
     * Tests that a simple event is as expected.
     *
     * @throws Exception
     */
    @Test
    public void simpleTest() throws Exception {
        FullFormatter form = new FullFormatter();
        assertEquals(eventString, form.format(event));
    }

    /**
     * Tests that a event with exception is as expected.
     *
     * @throws Exception
     */
    @Test
    public void exceptionTest() throws Exception {
        FullFormatter form = new FullFormatter();
        assertThat(form.format(eventWithException), startsWith(eventStringWithException));
    }

    /**
     * A micro benchmark.
     */
    @Test (timeout = 1000)
    public void microBenchmarkTest() {
        FullFormatter form = new FullFormatter();
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

    /**
     * A micro benchmark with exceptions.
     */
    @Test (timeout = 1000)
    public void microBenchmarkWithExceptionTest() {
        FullFormatter form = new FullFormatter();
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
