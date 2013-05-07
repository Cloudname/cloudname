package org.cloudname.timber.server;

import org.cloudname.log.pb.Timber;

import org.cloudname.timber.server.handler.LogEventHandler;

import org.jboss.netty.channel.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    // Queue for incoming log events and channels
    private final BlockingQueue<LogEventQueueEntry> incomingQueue;

    private final List<LogEventHandler> handlers;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private Thread consumerThread;
    private final CountDownLatch shutdownComplete = new CountDownLatch(1);

    // The acknowledgement manager
    private final AckManager ackManager = new AckManager();

    /**
     * @param incomingQueueLength the length of the input queue to the dispatcher.
     */
    public Dispatcher(int incomingQueueLength) {
        this.incomingQueueLength = incomingQueueLength;
        incomingQueue = new ArrayBlockingQueue<LogEventQueueEntry>(incomingQueueLength, true);
        handlers = new CopyOnWriteArrayList<LogEventHandler>();
    }

    /**
     * Initialize the dispatcher.  Creates a consumer thread.
     */
    public void init() {
        // Fire up the ackManager
        ackManager.init();

        // Fire up the consumer thread
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
     * Shut down the dispatcher.  Waits for the queue to be drained
     * and all the handlers to be closed before returning.
     */
    public void shutdown()
    {
        if (isShutdown.get()) {
            throw new IllegalStateException("Already called shutdown for dispatcher");
        }

        isShutdown.set(true);
        try {
            log.info("Waiting for queue to drain");
            shutdownComplete.await();
            log.info("Shutdown complete");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Shut down the ackManager.
        ackManager.shutdown();
    }

    /**
     * This method implements the consumer loop.  Poll the incoming
     * queue for events.  If the queue is empty.
     *
     */
    private void consumerLoop()
    {
        List<LogEventQueueEntry> rest = new ArrayList<LogEventQueueEntry>(incomingQueueLength);

        while (true) {
            LogEventQueueEntry entry = null;
            try {
                // Get next event off the queue.  Time out after
                // POLL_TIME milliseconds
                entry = incomingQueue.poll(POLL_TIME, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // TODO(borud): Not sure what to do when we are
                //   interrupted. Restarting the loop should be safe.
                continue;
            }

            // Process the event
            if (null != entry) {
                processEvent(entry);

                // If there is one event there is likely to be more,
                // so while we let the poll() operation take care of
                // the pacing we can opportunistically try to just
                // fetch the rest of the entries available to lower
                // contention.
                if (incomingQueue.size() > 0) {
                    incomingQueue.drainTo(rest);
                    for (LogEventQueueEntry ent : rest) {
                        processEvent(ent);
                    }
                    rest.clear();
                }

            }

            // Invariant: If we end up here it was because the queue
            //   was empty.  This is a good time to check if we have
            //   been shut down.
            if (isShutdown.get()) {
                // It is still possible that we were not shut down
                // when we called poll, and thus there could be
                // elements on the queue.  If this is the case we have
                // to drain the queue by re-doing the loop until we
                // are drained.
                if (! incomingQueue.isEmpty()) {
                    log.info("Shutdown called but queue was not empty");
                    continue;
                }

                log.fine("Shutdown called and queue verified to be empty");
                // If we are here we know we have been shut down and
                // that the queue has been drained. It is now safe to
                // shut down the handlers and exit.
                assert incomingQueue.isEmpty();
                for (LogEventHandler handler : handlers) {
                    log.info("Closing handler " + handler.getName());
                    handler.close();
                }
                log.info("exiting consumer loop");
                return;
            }
        }
    }

    /**
     * Take care of the actual dispatching of an event to the
     * registered handlers.  Will also determine if we can send back
     * an ACK to the client.
     *
     * The exception handling is a bit naive.  More thought needs to
     * go into what to do.  I guess we need some practical examples
     * first though.  For instance we might want to disable
     * misbehaving handlers after one or more errors.
     */
    private void processEvent(LogEventQueueEntry entry) {
        // Loop through the registered handlers and offer the event to them.
        Timber.LogEvent event = entry.getLogEvent();

        // If the message is a sync message from log shipment we do not
        // need to handle it, but we still need to ACK it. See TimberClientHandler
        // in Base for more information.
        boolean shouldHandle = true;
        if (event.hasId() &&
            event.getType().equals("C") &&
            event.getId().equals("sync")) {
            shouldHandle = false;
        }

        if (shouldHandle) {
            for (LogEventHandler handler : handlers) {
                try {
                    handler.handle(event);

                    // Anything other than consistency level BESTEFFORT
                    // means we are at a higher consistency level so we
                    // have to flush.
                    if (event.getConsistencyLevel() != Timber.ConsistencyLevel.BESTEFFORT) {
                        handler.flush();
                    }
                } catch (Exception e) {
                    log.log(Level.WARNING, "Got exception while dispatching to " + handler.getName(), e);
                }
            }
        }

        // Enqueue ack message if the message had an id
        if (event.hasId()) {
            // If dispatched from within process then channel might be
            // null and so it wouldn't make sense to send an ack.
            if (null == entry.getChannel()) {
                return;
            }

            ackManager.ack(entry.getChannel(), event);
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
     * @param channel the channel the event came from.  This is
     *   allowed to be {@code null}, but if it is, acknowledgements
     *   cannot be sent back this way.
     */
    public void dispatch(Timber.LogEvent logEvent, Channel channel)
    {
        if (isShutdown.get()) {
            throw new IllegalStateException("dispatcher was shut down");
        }
        try {
            incomingQueue.put(new LogEventQueueEntry(logEvent, channel));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Dispatch incoming log message.  This operation will block if
     * the input queue of the dispatcher is full.  This version of the
     * dispatch method does not take an originating channel and was
     * added to have a preferred method for injecting log events
     * internally.
     *
     * @param logEvent the log event we wish to enqueue.
     */
    public void dispatch(Timber.LogEvent logEvent) {
        dispatch(logEvent, null);
    }
}
