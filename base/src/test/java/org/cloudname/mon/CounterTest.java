package org.cloudname.mon;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit test for Counter.  This test is bound to look a bit weird
 * since we are testing counters managed by a singleton
 * MonitorManager.
 *
 * @author borud
 */
public class CounterTest {
    private static Counter counterOne = Counter.getCounter("test.cloudname.one");
    private static Counter counterTwo = Counter.getCounter("test.cloudname.two");

    @Test
    public void testSpinCounters() {

        // Make sure the counter is zero.
        assertEquals(0L, counterOne.getCount());

        // Just increase the counter a bunch of times.
        for (int i = 0; i < 100; i++) {
            long l = counterOne.inc();
            assertEquals(1, (l - i));
        }
        assertEquals(100L, counterOne.getCount());

        // Make sure counter is zero.
        assertEquals(0L, counterTwo.getCount());

        // Just increase the counter by i a bunch of times.
        for (int i = 0; i < 100; i++) {
            counterTwo.inc(i);
        }
        assertEquals(4950L, counterTwo.getCount());

        // make sure we find the counters.
        int hitCount = 0;
        for (String s : MonitorManager.getInstance().getCounterNames()) {
            if (s.equals("test.cloudname.one")) {
                assertEquals(counterOne, Counter.getCounter(s));
                hitCount++;
            }

            if (s.equals("test.cloudname.two")) {
                assertEquals(counterTwo, Counter.getCounter(s));
                hitCount++;
            }
        }

        assertEquals(2, hitCount);
    }
}