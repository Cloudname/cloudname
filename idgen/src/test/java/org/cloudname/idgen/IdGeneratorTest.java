package org.cloudname.idgen;

import java.util.logging.Logger;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for IdGenerator.
 *
 * @author borud.
 */
public class IdGeneratorTest {
    private static final Logger log = Logger.getLogger(IdGeneratorTest.class.getName());

    @Test
    public void testSimple() throws Exception {
        IdGenerator idgen = new IdGenerator(0L);
        long id = idgen.getNextId();
        assertTrue(id != 0);
    }

    @Test
    public void testMicroBenchmark() {
        IdGenerator idgen = new IdGenerator(0L);
        int numIterations = 100000;
        long start = System.currentTimeMillis();
        for (int i = 0; i < numIterations; ++i) {
            idgen.getNextId();
        }
        long duration = System.currentTimeMillis() - start;
        log.info("Microbenchmark: iterations = " + numIterations + ", time = " + duration + "ms");
    }

}