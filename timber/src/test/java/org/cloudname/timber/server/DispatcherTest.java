package org.cloudname.timber.server;

import org.cloudname.log.pb.Timber;
import static org.cloudname.log.pb.Timber.ConsistencyLevel;

import org.cloudname.timber.server.handler.LogEventHandler;
import org.cloudname.timber.server.handler.LogEventHandlerException;

import java.util.BitSet;
import com.google.protobuf.ByteString;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for Dispatcher.
 *
 * @author borud
 */
public class DispatcherTest {

    /**
     * LogEventHandler which emulates a slow handler.
     */
    private static class SlowHandler extends DummyHandler {
        public SlowHandler(String name) {
            super(name);
        }

        public void handle(Timber.LogEvent logEvent)
            throws LogEventHandlerException
        {
            super.handle(logEvent);
            try {
                // Emulate slow handler
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
    }

    /**
     * Utility method for creating a log message.
     * @return a Timber.LogEvent with a given text message.
     */
    public static Timber.LogEvent createMessage(String message) {
        return Timber.LogEvent.newBuilder()
            .setTimestamp(System.currentTimeMillis())
            .setConsistencyLevel(ConsistencyLevel.BESTEFFORT)
            .setLevel(1)
            .setHost("example.com")
            .setServiceName("myservice")
            .setSource(DispatcherTest.class.getName())
            .setPid(0)
            .setTid((int) Thread.currentThread().getId())
            .setType("T")
            .addPayload(
                Timber.Payload.newBuilder()
                .setName("msg")
                .setPayload(ByteString.copyFromUtf8(message)))
            .build();
    }

    /**
     * Trivial test with one slow handler.
     */
    @Test
    public void testDispatcherSimple() throws Exception {
        Dispatcher disp = new Dispatcher(10);
        MockChannel channel = new MockChannel();
        DummyHandler handler1 = new DummyHandler("DummyHandler 1");
        SlowHandler handler2 = new SlowHandler("DummyHandler 2");
        disp.addHandler(handler1);
        disp.addHandler(handler2);
        disp.init();

        for (int i = 0; i < 50; i++) {
            Timber.LogEvent event = createMessage("This is log message " + i);
            disp.dispatch(event, channel);
        }

        disp.shutdown();

        // Make sure the calls propagated
        assertEquals(50, handler1.getHandleCalled());
        assertEquals(50, handler2.getHandleCalled());
        assertEquals(1, handler1.getCloseCalled());
        assertEquals(1, handler2.getCloseCalled());
    }

    /**
     * Trivial test with one slow handler.
     */
    @Test
    public void testDispatcherWithoutChannel() throws Exception {
        DummyHandler handler = new DummyHandler("DummyHandler");
        Dispatcher disp = new Dispatcher(10);
        disp.addHandler(handler);
        disp.init();

        Timber.LogEvent event = createMessage("This is a log message");
        disp.dispatch(event);

        disp.shutdown();

        assertEquals(1, handler.getHandleCalled());
        assertEquals(1, handler.getCloseCalled());
    }

    /**
     * Verify that acknowledgements get written back on the channel.
     */
    @Test
    public void testAcknowledge() throws Exception {
        MockChannel channel = new MockChannel();
        assertNull(channel.getWriteObject());
        assertEquals(0, channel.getWriteCount());

        DummyHandler handler1 = new DummyHandler("dummy handler 1");
        DummyHandler handler2 = new DummyHandler("dummy handler 2");
        Dispatcher disp = new Dispatcher(10);
        disp.addHandler(handler1);
        disp.addHandler(handler2);
        disp.init();

        Timber.LogEvent eventWithConsistencySync = Timber.LogEvent.newBuilder(createMessage("meh"))
            .setConsistencyLevel(ConsistencyLevel.SYNC)
            .setId("abc123")
            .build();

        // Since the dispatcher is asynchronous we have to shut it
        // down to speed up proceedings a bit.  This is not elegant
        // but somewhat better than complicating the dispatcher with
        // synchronois queue draining.
        disp.dispatch(eventWithConsistencySync, channel);
        disp.shutdown();

        // Make sure we get exactly one ack.
        assertEquals(1, channel.getWriteCount());

        // Should throw a class cast exception if we get the wrong
        // type of object back.
        Timber.AckEvent ackEvent = (Timber.AckEvent) channel.getWriteObject();
        assertNotNull(ackEvent);
        assertEquals("abc123", ackEvent.getId(0));
        assertTrue(ackEvent.getTimestamp() > 0);
    }

    /**
     * Just test a bunch of events that have elevated consistencylevel.
     */
    @Test
    public void testMultipleAcknowledges() throws Exception {
        MockChannel channel = new MockChannel();
        assertNull(channel.getWriteObject());

        Dispatcher disp = new Dispatcher(10);
        DummyHandler handler = new DummyHandler("dummy handler");
        disp.addHandler(handler);
        disp.init();

        int numEvents = 1000;
        for (int i = 0; i < numEvents; i++) {
            Timber.LogEvent event = Timber.LogEvent.newBuilder(createMessage("meh " + i))
                .setConsistencyLevel(ConsistencyLevel.REPLICATED)
                .setId("" + i)
                .build();
            disp.dispatch(event, channel);
        }
        disp.shutdown();

        // Ensure the acks are there
        BitSet bits = new BitSet(1000);
        for (Object obj : channel.getObjects()) {
            Timber.AckEvent ackEvent = (Timber.AckEvent) obj;
            for (String id : ackEvent.getIdList()) {
                bits.set(Integer.parseInt(id));
            }
        }

        BitSet expected = new BitSet(1000);
        expected.set(0,1000);
        assertEquals(expected, bits);
    }
}
