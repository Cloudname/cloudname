package org.cloudname.timber.server;

import org.cloudname.log.pb.Timber;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * This class implements a per Channel queue for acknowledged IDs.
 * The queue has a fixed size and when the queue is full it will
 * gather all the ids that have been enqueued and write an AckEvent to
 * the Channel.
 *
 * This class is not thread safe.
 *
 * (The parts that deal with callbacks from Netty are thread safe)
 *
 * @author borud
 */
public class AckQueue {
    private static final Logger log = Logger.getLogger(AckQueue.class.getName());

    private final Channel channel;
    private final int queueSize;
    private final List<String> ids;
    private final Set<ChannelFuture> pendingWrites = new HashSet<ChannelFuture>();

    /**
     * Create an AckQueue.
     *
     * @param channel the channel we wish to send the acknowledgements to.
     * @param queueSize how many acknowledgements the queue should hold.
     */
    public AckQueue(Channel channel, int queueSize) {
        this.channel = channel;
        this.queueSize = queueSize;
        ids = new ArrayList<String>(queueSize);
    }

    /**
     * Enqueue an ack id.  If the queue is full this triggers a write
     * to the channel right away.  If the channel is not yet empty the
     * id is enqueued so that it can be written later.
     *
     * @param event the acknowledgement event we wish to send to the
     * channel.
     */
    public void enqueueAck(final Timber.LogEvent event) {
        ids.add(event.getId());

        if (ids.size() >= queueSize || event.getConsistencyLevel() != Timber.ConsistencyLevel.BESTEFFORT) {
            writeAckEvents();
        }
    }

    /**
     * @return the number of elements in the queue.
     */
    public int size() {
        return ids.size();
    }

    /**
     * Asynchronously write an AckEvent containing the acknowledged
     * IDs to the channel and empty the queue.
     */
    public void writeAckEvents() {
        if (0 == ids.size()) {
            // Nothing to write
            return;
        }

        Timber.AckEvent.Builder builder = Timber.AckEvent.newBuilder();
        builder.setTimestamp(System.currentTimeMillis());
        for (String id : ids) {
            builder.addId(id);
        }

        // Important, clear out the id queue.
        ids.clear();

        ChannelFuture future = channel.write(builder.build());

        // Add the future to the set of pending writes.
        synchronized(pendingWrites) {
            pendingWrites.add(future);
        }

        // Remove the future from the pending set upon
        // completion. Note that we do not check if the write actually
        // succeeded -- we only log if it failed.
        future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    synchronized(pendingWrites) {
                        pendingWrites.remove(future);
                    }

                    // If something went wrong we log it.
                    if (! future.isSuccess()) {
                        log.log(Level.INFO,
                                "Write to " + future.getChannel().toString() + " did not succeed",
                                future.getCause());
                    }
                }
            });
    }

    /**
     * Make sure that all ids that have been added up to this point
     * are written the client.
     *
     * @throws InterruptedException
     */
    public void flush() throws InterruptedException {
        writeAckEvents();

        // The events that were in the queue when we called flush have
        // either been removed from pendingWrites because they have
        // completed, or they are still there pending write, this
        // means that if we wait for whatever is left in the
        // pendingWrites set we will have flushed all the ids that had
        // been enqueued when we called flush.  Any ids that have been
        // added between our call to writeAckEvents() are of no
        // concern to this flush operation (but if they are flushed as
        // well, that's okay).
        Set<ChannelFuture> pendingFutures = new HashSet<ChannelFuture>();
        synchronized(pendingWrites) {
            pendingFutures.addAll(pendingWrites);
        }

        // Wait for each future in turn.
        for (ChannelFuture future : pendingFutures) {
            future.await();
        }
    }
}
