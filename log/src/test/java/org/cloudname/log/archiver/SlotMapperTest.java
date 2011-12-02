package org.cloudname.log.archiver;

import java.util.TimeZone;
import java.util.Calendar;
import java.util.GregorianCalendar;

import java.io.File;

import java.util.logging.Logger;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for SlotMapper.
 *
 * @author borud
 */
public class SlotMapperTest {
    private static final Logger log = Logger.getLogger(SlotMapperTest.class.getName());
    private static final long t1 = 1295872085000L;

    /**
     * Very simple test to make sure we get expected output.
     */
    @Test
    public void testMapping() throws Exception
    {
        SlotMapper mapper = new SlotMapper();

        String expected = "2011"
            + File.separator + "01"
            + File.separator + "24"
            + File.separator + "2011-01-24_12";

        assertEquals(expected, mapper.map(t1));
    }

    /**
     * Microbenchmark.  If this test fails you either have a
     * criminally slow machine or some changes have been made that
     * would adversely affect the performance.  The mapper is a
     * critical piece of code performance-wise since it will typically
     * be applied to <em>every</em> log message.
     */
    @Test (timeout = 1000)
    public void testPerformance() throws Exception
    {
        SlotMapper mapper = new SlotMapper();
        long start = System.currentTimeMillis();

        // Do 1M reps.
        int reps = 1000000;
        for (int i = 0; i < reps; i++) {
            // The multiplication factor of 5000 is there to space the
            // timestamps out a bit so that we exercise the slotCache
            // purging code path.
            assertNotNull(mapper.map(t1 + (i * 5000)));
        }

        long duration = System.currentTimeMillis() - start;
        log.fine("Mapping reps=" + reps + ", duration= " + duration + " ms ("
                 + ((reps / duration) * 1000) + " maps/sec)"
        );
    }

    /**
     * Make sure that for zero we get start of epoch.
     */
    @Test
    public void testForZero() throws Exception {
        // I seriously can't be arsed to adapt the unit test for
        // Windows.
        if (! "/".equals(File.separator)) {
            return;
        }

        SlotMapper mapper = new SlotMapper();
        assertEquals("1970/01/01/1970-01-01_00", mapper.map(0L));
    }

    /**
     * Negative timestamps should result in an exception being thrown.
     */
    @Test (expected = IllegalArgumentException.class)
    public void testNegative() throws Exception {
        SlotMapper mapper = new SlotMapper();
        mapper.map(-1L);
    }

    /**
     * Test rollover boundaries for hour, day, month, and year.
     * Duplicate the test for leap-years.
     */
    @Test
    public void testBoundaries() throws Exception
    {
        // I seriously can't be arsed to adapt the unit test for
        // Windows.
        if (! "/".equals(File.separator)) {
            return;
        }

        SlotMapper mapper = new SlotMapper();

        GregorianCalendar c1;
        GregorianCalendar c2;

        // Test hour rollover
        c1 = new GregorianCalendar(2011,0,20,1,59,59);
        c2 = new GregorianCalendar(2011,0,20,2,0,0);
        ensureDifferent(mapper, c1, c2);
        ensureConsistentMapping(mapper, c1);
        ensureConsistentMapping(mapper, c2);
        assertEquals("2011/01/20/2011-01-20_01", mapper.map(c1.getTimeInMillis()));
        assertEquals("2011/01/20/2011-01-20_02", mapper.map(c2.getTimeInMillis()));

        // Test day rollover
        c1 = new GregorianCalendar(2011,0,20,23,59,59);
        c2 = new GregorianCalendar(2011,0,21,0,0,0);
        ensureDifferent(mapper, c1, c2);
        ensureConsistentMapping(mapper, c1);
        ensureConsistentMapping(mapper, c2);
        assertEquals("2011/01/20/2011-01-20_23", mapper.map(c1.getTimeInMillis()));
        assertEquals("2011/01/21/2011-01-21_00", mapper.map(c2.getTimeInMillis()));


        // Test month rollover
        c1 = new GregorianCalendar(2011,0,31,23,59,59);
        c2 = new GregorianCalendar(2011,1,1,0,0,0);
        ensureDifferent(mapper, c1, c2);
        ensureConsistentMapping(mapper, c1);
        ensureConsistentMapping(mapper, c2);
        assertEquals("2011/01/31/2011-01-31_23", mapper.map(c1.getTimeInMillis()));
        assertEquals("2011/02/01/2011-02-01_00", mapper.map(c2.getTimeInMillis()));

        // Test month rollover, february, non-leap year
        c1 = new GregorianCalendar(2011,1,28,23,59,59);
        c2 = new GregorianCalendar(2011,2,1,0,0,0);
        ensureDifferent(mapper, c1, c2);
        ensureConsistentMapping(mapper, c1);
        ensureConsistentMapping(mapper, c2);
        assertEquals("2011/02/28/2011-02-28_23", mapper.map(c1.getTimeInMillis()));
        assertEquals("2011/03/01/2011-03-01_00", mapper.map(c2.getTimeInMillis()));

        // Test month rollover, february, leap year
        c1 = new GregorianCalendar(2012,1,29,23,59,59);
        c2 = new GregorianCalendar(2012,2,1,0,0,0);
        ensureDifferent(mapper, c1, c2);
        ensureConsistentMapping(mapper, c1);
        ensureConsistentMapping(mapper, c2);
        assertEquals("2012/02/29/2012-02-29_23", mapper.map(c1.getTimeInMillis()));
        assertEquals("2012/03/01/2012-03-01_00", mapper.map(c2.getTimeInMillis()));

        // Test year rollover
        c1 = new GregorianCalendar(2011,11,31,23,59,59);
        c2 = new GregorianCalendar(2012,0,1,0,0,0);
        ensureDifferent(mapper, c1, c2);
        ensureConsistentMapping(mapper, c1);
        ensureConsistentMapping(mapper, c2);
        assertEquals("2011/12/31/2011-12-31_23", mapper.map(c1.getTimeInMillis()));
        assertEquals("2012/01/01/2012-01-01_00", mapper.map(c2.getTimeInMillis()));
    }

    /**
     * Make sure that the dates are mapped to different slots.
     */
    private void ensureDifferent(SlotMapper mapper,
                                 GregorianCalendar cal1,
                                 GregorianCalendar cal2)
    {
        cal1.setTimeZone(SlotMapper.TZ);
        cal2.setTimeZone(SlotMapper.TZ);

        long t1 = cal1.getTimeInMillis();
        long t2 = cal2.getTimeInMillis();

        assertNotSame(mapper.map(t1),
                      mapper.map(t2));
    }

    /**
     * Make sure that the cached mappings agree with calling the mapToPath
     * method.
     */
    private void ensureConsistentMapping(SlotMapper mapper, GregorianCalendar cal)
    {
        assertEquals(SlotMapper.mapToPath(cal.getTimeInMillis()),
                     mapper.map(cal.getTimeInMillis()));
    }
}
