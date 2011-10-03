package org.cloudname.timber.server;

import org.cloudname.log.pb.Timber;

import org.cloudname.timber.server.handler.LogEventHandler;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * This class implements the main dispatcher for LogEvents.
 *
 * Right now this class is really primitive and probably won't offer
 * much in terms of performance or safety features.  The dispatcher
 * has an incoming queue of a fixed size.
 *
 * @author borud
 */
public class Dispatcher {
    private static final Logger log = Logger.getLogger(Dispatcher.class.getName());

    // How long to wait for elements to appear on incoming queue.
    private static int POLL_TIME = 500;

    private int incomingQueueLength;
    private final BlockingQueue<Timber.LogEvent> incomingQueue;
    private final List<LogEventHandler> handlers;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private Thread consumerThread;
    private final CountDownLatch shutdownComplete = new CountDownLatch(1);

    /**
     * @param incomingQueueLength the length of the input queue to the dispatcher.
     */
    public Dispatcher(int incomingQueueLength)
    {
        this.incomingQueueLength = incomingQueueLength;
        incomingQueue = new ArrayBlockingQueue<Timber.LogEvent>(incomingQueueLength, true);
        handlers = new CopyOnWriteArrayList<LogEventHandler>();
    }

    /**
     * Initialize the dispatcher.  Creates a consumer thread.
     */
    public void init() {
        consumerThread = new Thread(new Runnable() {
                public void run() {
                    log.fine("Starting consumer");
                    consumerLoop();
                    shutdownComplete.countDown();
                    log.fine("Consumer shut down");
                }
            });
        consumerThread.start();
    }


    /**
     * This method implements the consumer loop.  Poll the incoming
     * queue for events.  If the queue is empty
     */
    private void consumerLoop()
    {
        while (true) {
            try {
                // Get next event off the queue.  Time out
                // after POLL_TIME milliseconds
                Timber.LogEvent event = incomingQueue.poll(POLL_TIME,
                                                           TimeUnit.MILLISECONDS);
                // Check if we got an event or if we timed out
                if (null != event) {
                    // Loop through the registered handlers and
                    // offer the event to them.
                    for (LogEventHandler handler : handlers) {
                        handler.handle(event);
                    }
                } else {
                    // If we end up here it was because the queue was
                    // empty.  This is a good time to check if we have
                    // been shut down.
                    if (isShutdown.get()) {
                        // It is still possible that we were not shut
                        // down when we called poll, and thus there
                        // could be elements on the queue.  If this is
                        // the case we have to drain the queue by
                        // re-doing the loop until we are drained.
                        if (! incomingQueue.isEmpty()) {
                            log.info("Shutdown called but queue was not empty");
                            continue;
                        }

                        log.fine("Shutdown called and queue verified to be empty");
                        // If we are here we know we have been shut
                        // down and that the queue has been
                        // drained. It is now safe to shut down the
                        // handlers and exit.
                        assert incomingQueue.isEmpty();
                        for (LogEventHandler handler : handlers) {
                            log.info("Closing handler " + handler.getName());
                            handler.close();
                        }
                        log.info("exiting consumer loop");
                        return;
                    }
                }
            } catch (InterruptedException e) {
                // If we are interrupted that probably means we
                // need to shut down?
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Add handler to dispatcher.
     *
     * @param handler the handler we wish to add.
     * @return this reference for chaining.
     * @throws NullPointerException if handler is null.
     * @throws IllegalStateException if handler was already added.
     */
    public Dispatcher addHandler(LogEventHandler handler)
    {
        if (null == handler) {
            throw new NullPointerException("handler cannot be null");
        }

        if (handlers.contains(handler)) {
            throw new IllegalStateException("handler was already added");
        }

        handlers.add(handler);
        log.info("Added handler " + handler.getName());
        return this;
    }

    /**
     * Dispatch incoming log message.  This operation will block if
     * the input queue of the dispatcher is full.
     *
     * @param logEvent the log event we wish to enqueue.
     */
    public void dispatch(Timber.LogEvent logEvent)
    {
        if (isShutdown.get()) {
            throw new IllegalStateException("dispatcher was shut down");
        }
        try {
            incomingQueue.put(logEvent);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Shut down the dispatcher.  Waits for the queue to be drained
     * and all the handlers to be closed before returning.
     */
    public void shutdown()
    {
        isShutdown.set(true);
        try {
            log.info("Waiting for queue to drain");
            shutdownComplete.await();
            log.info("Shutdown complete");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}