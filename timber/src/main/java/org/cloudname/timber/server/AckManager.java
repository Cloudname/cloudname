package org.cloudname.timber.server;

import org.cloudname.log.pb.Timber;

import org.jboss.netty.channel.Channel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * The AckManager class implements the mechanism for sending
 * acknowledgements back to the logging client.
 *
 * We usually do not send back acknowledgements right away, we allow
 * them to accumulate and then either send them back after some delay
 * (usually in the range 100-200ms) or when the outbound queue reaches
 * its max size.
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
    private static final int ACKNOWLEDGE_QUEUE_SIZE = 30;

    // Indicate whether we wish to shut down.
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    // The incoming queue for acknowledgements.
    private final BlockingQueue<AckEntry> incomingQueue = new ArrayBlockingQueue<AckEntry>(INCOMING_QUEUE_LENGTH);

    // Map from channel to AckQueue
    private Map<Channel, AckQueue> channelQueueMap = new HashMap<Channel, AckQueue>();

    private Thread consumerThread;

    // Acknowledgement queue entry.
    private static class AckEntry {
        private final Channel channel;
        private final Timber.LogEvent event;

        public AckEntry(final Channel channel, final Timber.LogEvent event) {
            this.channel = channel;
            this.event = event;
        }

        public Channel getChannel() {
            return channel;
        }

        public Timber.LogEvent getEvent() {
            return event;
        }
    }

    /**
     * Initialize the AckManager.  Fires off a thread that deals with
     * the consumer loop.
     */
    public void init() {
        consumerThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    log.info("Starting AckManager");
                    consumerLoop();
                    log.info("Shutting down AckManager...");
                }
            });
        consumerThread.start();
    }

    /**
     * Shut down the AckManager.
     */
    public void shutdown() {
        isShutdown.set(true);
        try {
            consumerThread.join();
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
     * @param event the LogEvent we want to acknowledge.
     */
    public void ack(Channel channel, Timber.LogEvent event) {
        if (isShutdown.get()) {
            throw new IllegalStateException("Cannot enqueue ack after AckManager has shut down");
        }

        while (true) {
            try {
                // put() waits if the queue is full.
                incomingQueue.put(new AckEntry(channel, event));
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
     *
     * TODO(borud): the timing of processQueues() needs to be
     *   tightened up.
     */
    private void consumerLoop() {
        long lastPeriodicProcess = System.currentTimeMillis();

        // List to hold extra entries to speed up reading from the
        // incoming queue.
        List<AckEntry> rest = new ArrayList<AckEntry>(INCOMING_QUEUE_LENGTH);

        while (true) {
            // We want the periodic processing to take place at most
            // every QUEUE_POLL_TIME milliseconds.
            final long now = System.currentTimeMillis();
            if ((now - lastPeriodicProcess) > QUEUE_POLL_TIME) {
                processQueues();
                lastPeriodicProcess = now;
            }

            final AckEntry entry;
            try {
                entry = incomingQueue.poll(QUEUE_POLL_TIME, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore and try again.
                continue;
            }

            if (null != entry) {
                // Got an event, process it
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
            }

            if (isShutdown.get()) {
                // It is still possible that we were not shut down
                // when we called poll, and thus there could be
                // elements on the queue.  If this is the case we have
                // to drain the queue by re-doing the loop until we
                // are drained.
                if (! incomingQueue.isEmpty()) {
                    continue;
                }

                assert incomingQueue.isEmpty();

                // Push pending acks and wait for writes to complete.
                // I know this is a weird place to do the final
                // flushing, but we do it here to avoid having to do
                // any locking (ie. we do it in the thread that deals
                // with all the queues).
                flush();

                // Bail out.
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
     * <b>Should only be called from consumerLoop().</b>
     *
     */
    private void processIncoming(AckEntry entry) {
        Channel channel = entry.getChannel();
        AckQueue queue = channelQueueMap.get(channel);
        if (null == queue) {
            queue = new AckQueue(channel, ACKNOWLEDGE_QUEUE_SIZE);
            channelQueueMap.put(channel, queue);
        }

        queue.enqueueAck(entry.getEvent());
    }

    /**
     * Iterate over all channelQueueMap to remove channels that have
     * been closed, and to perform writes on channels
     *
     * <b>Should only be called from consumerLoop().</b>
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
                queue.writeAckEvents();
            }
        }

        // Dispose the queues that are associated to closed channels.
        for (Channel channel : disposableChannels) {
            AckQueue queue = channelQueueMap.remove(channel);
            log.info("Disposed channels " + channel.toString() + " with " + queue.size() + " ids still in it");
        }
    }

    /**
     * Flush all the channels.  Blocks until all network writes have
     * finished.
     *
     * <b>Should only be called from consumerLoop().</b>
     *
     */
    private void flush() {
        for (AckQueue queue : channelQueueMap.values()) {
            labelwhile:
            while(true) {
                try {
                    queue.flush();
                    break labelwhile;
                } catch(InterruptedException e) {
                    // Ignore
                }
            }
        }
    }
}
