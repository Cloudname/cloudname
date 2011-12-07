package org.cloudname.timber.server;

import org.cloudname.log.pb.Timber;

import org.jboss.netty.channel.Channel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * The AckManager class implements the mechanism for sending
 * acknowledgements back to the logging client.
 *
 * We usually do not send back acknowledgements right away, we allow
 * them to accumulate and then either send them back after
 * ACKNOWLEDGE_PERIOD milliseconds have passed or a channel has
 * reached ACKNOWLEDGE_AFTER.  This way we can bunch together
 * acknowledgements for multiple incoming messages into fewer packets.
 *
 * This class has not been optimized yet.  There are lots of ways to
 * optimize this class.
 */
public class AckManager {
    private static final Logger log = Logger.getLogger(AckManager.class.getName());

    // Incoming queue length
    private static final int INCOMING_QUEUE_LENGTH = 100;

    // How often do we flush the acknowledgements
    private static final int QUEUE_POLL_TIME = 500;

    // The number of queued acknowledgements to keep for a channel
    // before we flush acknowledgements for that channel.
    private static final int ACKNOWLEDGE_QUEUE_SIZE = 50;

    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private Thread consumerThread;
    private final CountDownLatch shutdownComplete = new CountDownLatch(1);

    // Acknowledgement queue entry.
    private static class AckQueueEntry {
        private Channel channel;
        private String id;

        public AckQueueEntry(Channel channel, String id) {
            this.channel = channel;
            this.id = id;
        }

        public Channel getChannel() {
            return channel;
        }

        public String getId() {
            return id;
        }
    }

    // The incoming queue for acknowledgements
    private final BlockingQueue<AckQueueEntry> incomingQueue
        = new ArrayBlockingQueue<AckQueueEntry>(INCOMING_QUEUE_LENGTH);

    /**
     * Initialize the AckManager.
     */
    public void init() {
        consumerThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    log.info("Starting consumer for AckManager");
                    consumerLoop();
                    shutdownComplete.countDown();
                    log.info("Consumer for AckManager shut down");
                }
            });
    }

    /**
     * Shut down the AckManager.
     */
    public void shutdown() {
        isShutdown.set(true);
        try {
            log.info("Waiting for incoming queue to drain");
            shutdownComplete.await();
            log.info("Shutdown of AckManager complete");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Enqueue an acknowledgement on a given channel.  Note that this
     * method will block if the acknowledgement queue is full.
     *
     * @param channel the channel we want to send the acknowledgement on.
     * @param id the LogEvent id we want to acknowledge.
     */
    public void enqueueAck(Channel channel, String id) {
        if (isShutdown.get()) {
            throw new IllegalStateException("Cannot enqueue ack after AckManager has shut down");
        }

        // TODO(borud): there has got to be a better way to handle
        // this.
        while (true) {
            try {
                // put() waits if the queue is full.
                incomingQueue.put(new AckQueueEntry(channel, id));
                return;
            } catch (InterruptedException e) {
                // NOP
            }
        }
    }

    /**
     * Consume incoming acknowledgements and stash them into a per
     * channel queue so that we can bunch together acknowledgements
     * into fewer packets.
     */
    private void consumerLoop() {
        while (true) {
            AckQueueEntry entry = null;
            try {
                entry = incomingQueue.poll(QUEUE_POLL_TIME, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // If we are interrupted that probably means we
                // need to shut down?
                Thread.currentThread().interrupt();
                return;
            }

            // Got an event, process it
            if (null != entry) {
                processIncoming(entry);
                continue;
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
                assert incomingQueue.isEmpty();
                log.info("exiting consumer loop");
                return;
            }
        }
    }

    private void processIncoming(AckQueueEntry entry) {
        log.info("Entry channel=" + entry.getChannel().toString() + ", id=" + entry.getId());
    }

}