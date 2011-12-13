package org.cloudname.timber.client;

import java.net.InetAddress;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit test for ReconnectDelayManager.
 *
 * @author borud
 */
public class ReconnectDelayManagerTest {
    // Statics
    private static InetAddress address;
    private final static int initialDelay = 100;
    private final static int maxDelay = 10000;
    private final static int resetTime = 20000;

    // Per instance state
    private SettableTimeProvider timeProvider;
    private ReconnectDelayManager delayManager;

    @BeforeClass
    public static void setUpClass() throws Exception {
        address = InetAddress.getLocalHost();
    }

    @Before
    public void setUp() throws Exception {
        timeProvider = new SettableTimeProvider();
        timeProvider.setTime(System.currentTimeMillis());

        delayManager = new ReconnectDelayManager(
            initialDelay,
            maxDelay,
            resetTime,
            timeProvider
        );
    }

    /**
     * A TimeProvider that allows us to set the time.  This provider
     * is initialized to time 0.
     */
    private static class SettableTimeProvider implements ReconnectDelayManager.TimeProvider {
        long time = 0;

        public void setTime(long time) {
            this.time = time;
        }

        public long currentTimeMillis() {
            return time;
        }
    }

    /**
     *
     */
    @Test
    public void testSimple() throws Exception {
        // Loop through with clock that is always constant.
        ArrayList<Integer> values = new ArrayList<Integer>();
        int delay = 0;
        while (delay < maxDelay) {
            delay = delayManager.getReconnectDelayForAddress(address);
            values.add(delay);
        }

        // Make sure we get the expected sequence.
        List<Integer> expected = Arrays.asList(100, 200, 400, 800, 1600, 3200, 6400, 10000);
        assertEquals(expected, values);

        // Make sure that next delay is also equal to maxDelay
        assertEquals(maxDelay, delayManager.getReconnectDelayForAddress(address));
    }

    /**
     * Simulate that we request reconnect until we reach maxDelay in
     * rapid succession (constant clock), then advance the clock by
     * resetTime and ensure that the next value we get is
     * initialDelay.
     */
    @Test
    public void testResetDelay() throws Exception {
        // Set clock to known point
        long now = System.currentTimeMillis();
        timeProvider.setTime(now);

        // Request delay time until max is reached
        int delay = 0;
        while (delay < maxDelay) {
            delay = delayManager.getReconnectDelayForAddress(address);
        }

        // Advance the clock by resetTime milliseconds
        now += resetTime;
        timeProvider.setTime(now);

        // Now bump the time up by resetTime and ensure that this
        // gives us a delay that is equal to initialDelay
        assertEquals(initialDelay, delayManager.getReconnectDelayForAddress(address));
        assertEquals(initialDelay * 2, delayManager.getReconnectDelayForAddress(address));
    }

    /**
     * Test constructor with invalid initialReconnectDelay parameter.
     */
    @Test (expected = IllegalArgumentException.class)
    public void testIllegalInitialDelay() {
        new ReconnectDelayManager(-1, maxDelay, resetTime, timeProvider);
    }

    /**
     * Test constructor with invalid maxReconnectDelay parameter.
     */
    @Test (expected = IllegalArgumentException.class)
    public void testIllegalMaxReconnectDelay() {
        new ReconnectDelayManager(initialDelay, initialDelay - 1, resetTime, timeProvider);
    }

    /**
     * Test constructor with invalid reconnectDelayReset parameter.
     */
    @Test (expected = IllegalArgumentException.class)
    public void testIllegalReconnectDelayReset() {
        new ReconnectDelayManager(initialDelay, maxDelay, maxDelay - 1, timeProvider);
    }
}