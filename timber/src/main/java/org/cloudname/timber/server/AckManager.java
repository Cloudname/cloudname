package org.cloudname.timber.server;

import org.cloudname.log.pb.Timber;

import org.jboss.netty.channel.Channel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
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
    private static final int INCOMING_QUEUE_LENGTH = 1000;

    // How often do we flush the acknowledgements
    private static final int QUEUE_POLL_TIME = 100;

    // The number of queued acknowledgements to keep for a channel
    // before we flush acknowledgements for that channel.
    private static final int ACKNOWLEDGE_QUEUE_SIZE = 50;

    // Indicate whether we wish to shut down.
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    // When the consumer thread exits it will count down this latch.
    private final CountDownLatch shutdownComplete = new CountDownLatch(1);

    // The incoming queue for acknowledgements
    private final BlockingQueue<AckEntry> incomingQueue = new ArrayBlockingQueue<AckEntry>(INCOMING_QUEUE_LENGTH);

    // Map from channel to AckQueue
    private Map<Channel, AckQueue> channelQueueMap = new HashMap<Channel, AckQueue>();

    // Acknowledgement queue entry.
    private static class AckEntry {
        private Channel channel;
        private String id;

        public AckEntry(Channel channel, String id) {
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

    /**
     * Initialize the AckManager.
     */
    public void init() {
        new Thread(new Runnable() {
                @Override
                public void run() {
                    log.info("Starting consumer for AckManager");
                    consumerLoop();
                    shutdownComplete.countDown();
                    log.info("Consumer for AckManager shut down");
                }
            }).start();
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
    public void ack(Channel channel, String id) {
        if (isShutdown.get()) {
            throw new IllegalStateException("Cannot enqueue ack after AckManager has shut down");
        }

        while (true) {
            try {
                // put() waits if the queue is full.
                incomingQueue.put(new AckEntry(channel, id));
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
        long lastPeriodicProcess = System.currentTimeMillis();

        // List to hold extra entries to speed up reading from the
        // incoming queue.
        List<AckEntry> rest = new ArrayList<AckEntry>(INCOMING_QUEUE_LENGTH);

        while (true) {
            // We want the periodic processing to take place at most
            // every QUEUE_POLL_TIME milliseconds.
            long now = System.currentTimeMillis();
            if ((now - lastPeriodicProcess) > QUEUE_POLL_TIME) {
                processQueues();
                lastPeriodicProcess = now;
            }


            AckEntry entry = null;
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

                // If there is more, drain the rest of the
                // queue. Gives a slight speedup, but is unfortunately
                // butt ugly
                if (incomingQueue.size() > 0) {
                    incomingQueue.drainTo(rest);
                    for (AckEntry ent : rest) {
                        processIncoming(ent);
                    }
                    rest.clear();
                }
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

                log.fine("Shutdown called and incoming queue verified to be empty");
                assert incomingQueue.isEmpty();

                // TODO(borud): At this point we should drain the
                //   acknowledgement queues as well, but this requires
                //   a bit of re-thinking so we will fix this later.

                log.info("exiting consumer loop");
                return;
            }
        }
    }

    /**
     * Process an incoming AckEntry.  Sorts incoming ack ids into
     * queues keyed by the Channel they arrived on.  If a queue is
     * full, then we send an ack packet back to the client.  If the
     * queue is not full we wait until the periodic draining takes
     * place.
     *
     * Should only be called from consumerLoop().
     *
     */
    private void processIncoming(AckEntry entry) {
        Channel channel = entry.getChannel();
        AckQueue queue = channelQueueMap.get(channel);
        if (null == queue) {
            queue = new AckQueue(channel, ACKNOWLEDGE_QUEUE_SIZE);
            channelQueueMap.put(channel, queue);
        }

        queue.enqueueAckId(entry.getId());
    }

    /**
     * Iterate over all channelQueueMap to remove channels that have
     * been closed, and to perform writes on channels where the
     * RESPONSE_DELAY_TIME has been reached.
     *
     * Should only be called from consumerLoop().
     */
    private void processQueues() {
        List<Channel> disposableChannels = new LinkedList<Channel>();

        for (Map.Entry<Channel, AckQueue> ent : channelQueueMap.entrySet()) {
            Channel channel = ent.getKey();
            AckQueue queue = ent.getValue();

            // Check for channels that have closed
            if (! channel.isOpen()) {
                disposableChannels.add(channel);
                continue;
            }

            // Drain queues that have data in them
            if (queue.size() > 0) {
                log.info("Write initiated for " + channel.toString());
                queue.writeAckEvents();
            }
        }

        // Dispose the queues that are associated to closed channels.
        for (Channel channel : disposableChannels) {
            AckQueue queue = channelQueueMap.remove(channel);
            log.info("Disposed channels " + channel.toString() + " with " + queue.size() + " ids still in it");
        }
    }
}