package org.cloudname.timber.server;

import org.cloudname.log.pb.Timber;

import org.jboss.netty.channel.Channel;

import org.junit.*;
import static org.junit.Assert.*;

import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.CountDownLatch;

/**
 * Unit tests for the AckQueue class.
 *
 * @author borud
 */
public class AckQueueTest {
    private static final Logger log = Logger.getLogger(AckQueue.class.getName());

    private static int queueSize = 20;
    private MockChannel mockChannel;
    private AckQueue queue;


    @Before
    public void setUp() throws Exception {
        mockChannel = new MockChannel();
        queue = new AckQueue(mockChannel, queueSize);
    }

    @Test
    public void testSimple() throws Exception {
        queue.enqueueAckId("123");
        assertEquals(1, queue.size());
        assertNull(mockChannel.getWriteObject());

        queue.flush();

        // Make sure there are no elements in the queue.
        assertEquals(0, queue.size());

        // Make sure that we have gotten the object
        assertNotNull(mockChannel.getWriteObject());

        // And make sure it is the right object
        assertEquals("123", ((Timber.AckEvent) mockChannel.getWriteObject()).getId(0));
    }

    @Test
    public void testWriteOnFullQueue() throws Exception {
        // The number of IDs we feed in is one short of queueSize so
        // as not to trigger the automatic writing.
        int numIds = queueSize - 1;

        for (int i = 0; i < numIds; i++) {
            queue.enqueueAckId("" + i);
        }

        // Make sure we still have all the elements in the queue
        assertEquals(numIds, queue.size());

        // ..and that none have been written to the mock channel
        assertEquals(0, mockChannel.getWriteCount());

        // now push the channel over the edge
        queue.enqueueAckId("" + numIds);

        // since the writing takes place in the current thread the
        // queue should now be empty.
        assertEquals(0, queue.size());

        // There should be exactly one write on the mock channel
        assertEquals(1, mockChannel.getWriteCount());

        // And the last write should be our AckEvent
        Timber.AckEvent ackEvent = (Timber.AckEvent) mockChannel.getWriteObject();

        // Make sure everything is there
        BitSet bitVector = new BitSet(numIds);
        for (String id : ackEvent.getIdList()) {
            bitVector.set(Integer.parseInt(id));
        }

        for (int i = 0; i <= numIds; i++) {
            assertTrue(bitVector.get(i));
        }
    }
}