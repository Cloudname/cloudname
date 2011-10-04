package org.cloudname.timber.server;

import org.cloudname.log.pb.Timber;

import org.cloudname.timber.server.handler.LogEventHandler;
import org.cloudname.timber.server.handler.LogEventHandlerException;

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
    public void testDispatcherSimple()
    {
        Dispatcher disp = new Dispatcher(10);
        DummyHandler handler1 = new DummyHandler("DummyHandler 1");
        SlowHandler handler2 = new SlowHandler("DummyHandler 2");
        disp.addHandler(handler1);
        disp.addHandler(handler2);
        disp.init();

        for (int i = 0; i < 50; i++) {
            Timber.LogEvent event = createMessage("This is log message " + i);
            disp.dispatch(event);
        }

        disp.shutdown();

        // Make sure the calls propagated
        assertEquals(50, handler1.getHandleCalled());
        assertEquals(50, handler2.getHandleCalled());
        assertEquals(1, handler1.getCloseCalled());
        assertEquals(1, handler2.getCloseCalled());
    }
}