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

    @Test
    public void testFeedingAcks() throws Exception {
        int numIterations = 1000000;
        AckManager manager = new AckManager();
        Channel channel = new MockChannel();

        manager.init();

        long start = System.currentTimeMillis();
        for (int i = 0; i < numIterations; i++) {
            manager.ack(channel, "id" + i);
        }
        long duration1 = System.currentTimeMillis() - start;
        manager.shutdown();
        long duration2 = System.currentTimeMillis() - start;

        log.info("### numIterations = " + numIterations);
        log.info("###     duration1 = " + duration1);
        log.info("###     duration1 = " + duration2);

        manager.shutdown();
    }
}