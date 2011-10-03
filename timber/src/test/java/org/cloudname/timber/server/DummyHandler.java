package org.cloudname.timber.server;

import org.cloudname.log.pb.Timber;

import java.util.concurrent.atomic.AtomicInteger;

import org.cloudname.timber.server.handler.LogEventHandler;
import org.cloudname.timber.server.handler.LogEventHandlerException;

/**
 * Dummy LogEventHandler used for testing.
 */
public class DummyHandler implements LogEventHandler {
    private AtomicInteger flushCalled = new AtomicInteger(0);
    private AtomicInteger closeCalled = new AtomicInteger(0);
    private AtomicInteger handleCalled = new AtomicInteger(0);
    private String name;

    public DummyHandler(String name) {
        this.name = name;
    }

    @Override
    public void handle(Timber.LogEvent logEvent)
        throws LogEventHandlerException
    {
        handleCalled.incrementAndGet();
    }

    @Override
    public void flush() {
        flushCalled.incrementAndGet();
    }

    @Override
    public void close() {
        closeCalled.incrementAndGet();
    }

    @Override
    public String getName() {
        return name;
    }

    public int getFlusheCalled() {
        return flushCalled.get();
    }

    public int getCloseCalled() {
        return closeCalled.get();
    }

    public int getHandleCalled() {
        return handleCalled.get();
    }
}


