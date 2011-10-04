package org.cloudname.mon;

import static org.junit.Assert.assertEquals;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

/**
 * Unit tests for HistogramCounter
 *
 * @author borud, espen
 */
public class HistogramCounterTest {

    /**
     * Kick-the-tyres test.
     */
    @Test
    public void testHistogram() {
        HistogramCounter h = HistogramCounter.getHistogramCounter("histogram1");
        h.setCeilings(
            Arrays.asList(10L,
                          100L,
                          1000L,
                          10000L,
                          100000L));


        Random random = new Random();

        for (int i = 0; i < 10000; i++) {
            h.count(Math.abs(random.nextLong() % 100000L));
        }
    }

    @Test
    public void testGetEntries() {
        HistogramCounter h = HistogramCounter.getHistogramCounter("histogram2");
        h.setCeilings(Arrays.asList(1L, 5L, 10L));
        for (long lo = 0; lo < 11; lo++) {
            h.count(lo);
        }

        // Create the List we check against
        List<Map.Entry<Long,Long>> correct = new ArrayList<Map.Entry<Long,Long>>(4);
        correct.add(new AbstractMap.SimpleEntry<Long, Long>(1L, 2L));
        correct.add(new AbstractMap.SimpleEntry<Long, Long>(5L, 4L));
        correct.add(new AbstractMap.SimpleEntry<Long, Long>(10L, 5L));
        correct.add(new AbstractMap.SimpleEntry<Long, Long>(Long.MAX_VALUE, 0L));

        assertEquals(correct, h.getEntries());
        
        System.out.println(h.toString());
    }
}