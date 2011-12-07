package org.cloudname.timber.server;

import org.jboss.netty.channel.Channel;

import java.util.logging.Logger;

import org.junit.*;
import org.junit.Assert.*;

/**
 * Unit tests for AckManager.
 *
 * @author borud
 */
public class AckManagerTest {
    private static final Logger log = Logger.getLogger(AckManagerTest.class.getName());

    /**
     * Trivial instantiation and start/stop test.
     */
    @Test
    public void testSimple() throws Exception {
        AckManager manager = new AckManager();
        manager.init();
        manager.shutdown();
    }

    @Test (timeout = 1000)
    public void testFeedingAcks() throws Exception {
        int numIterations = 10000;
        AckManager manager = new AckManager();
        Channel channel = new MockChannel();

        manager.init();

        long start = System.currentTimeMillis();
        for (int i = 0; i < numIterations; i++) {
            manager.ack(channel, "id" + i);
        }
        long duration = System.currentTimeMillis() - start;
        log.info("Processed " + numIterations + " acknowledgements in "+ duration + "ms (" + (numIterations / ( (double)duration / 1000)) + " per sec)");
        manager.shutdown();

    }
}