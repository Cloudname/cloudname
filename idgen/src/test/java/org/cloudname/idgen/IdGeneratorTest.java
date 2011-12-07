package org.cloudname.idgen;

import java.util.logging.Logger;
import java.util.Set;
import java.util.HashSet;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for IdGenerator.
 *
 * @author borud.
 */
public class IdGeneratorTest {
    private static final Logger log = Logger.getLogger(IdGeneratorTest.class.getName());

    /**
     * A time provider that lets us set time.
     */
    private static class SettableTimeprovider implements TimeProvider {
        long t = 0;
        @Override
        public long getTimeInMillis() {
            return t;
        }

        public SettableTimeprovider setTime(long t) {
            this.t = t;
            return this;
        }
    }


    @Test
    public void testSimple() throws Exception {
        IdGenerator idgen = new IdGenerator(0L);
        long id = idgen.getNextId();
        assertTrue(id != 0);
        assertNotNull(idgen.getNextIdHex());
    }

    @Test (timeout=50)
    public void testMicroBenchmark() {
        IdGenerator idgen = new IdGenerator(0L);
        int numIterations = 10000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < numIterations; ++i) {
            idgen.getNextId();
        }
        long duration = System.currentTimeMillis() - start;
        log.info("Microbenchmark: iterations = " + numIterations + ", time = " + duration + "ms");
    }

    /**
     * Test that we can cope with a clock that sometimes jumps 10ms
     * backwards in time.  Also ensure that the IDs generated are
     * unique.
     */
    @Test
    public void testBackwardsClock() {
        int numIterations = 10000;
        Set<Long> idSet = new HashSet<Long>(numIterations);

        // Time provider which jumps 5ms back in time every 109 calls.
        TimeProvider tp = new TimeProvider() {
                private int counter = 0;
                @Override
                public long getTimeInMillis() {
                    if ((counter++ % 109) == 0) {
                        return System.currentTimeMillis() - 5;
                    }
                    return System.currentTimeMillis();
                }
            };

        IdGenerator idgen = new IdGenerator(0L, tp);
        for (int i = 0; i < numIterations; ++i) {
            assertTrue(idSet.add(idgen.getNextId()));
        }
        assertEquals(numIterations, idSet.size());
    }
}
