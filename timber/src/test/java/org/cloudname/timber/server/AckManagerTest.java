package org.cloudname.timber.server;

import org.cloudname.log.pb.Timber;
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

    private Timber.LogEvent.Builder getDummyLogEventBuilder() {
        return Timber.LogEvent.newBuilder().setTimestamp(System.currentTimeMillis())
            .setConsistencyLevel(Timber.ConsistencyLevel.SYNC)
            .setLevel(0)
            .setHost("host")
            .setServiceName("service")
            .setSource("source")
            .setType("type");
    }

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
        Timber.LogEvent e;
        for (int i = 0; i < numIterations; i++) {
            e = getDummyLogEventBuilder().setId("id" + i).build();
            manager.ack(channel, e);
        }
        long duration = System.currentTimeMillis() - start;
        log.info("Processed " + numIterations + " acknowledgements in "+ duration + "ms (" + (numIterations / ( (double)duration / 1000)) + " per sec)");
        manager.shutdown();

    }
}
