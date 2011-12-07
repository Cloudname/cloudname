package org.cloudname.timber.server;

import org.cloudname.log.pb.Timber;
import org.jboss.netty.channel.Channel;

import java.util.ArrayList;
import java.util.List;

/**
 * This class implements a per Channel queue for acknowledged IDs.
 *
 * @author borud
 */
public class AckQueue {
    private final Channel channel;
    private final int queueSize;
    private final List<String> ids;

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
     * @param id the acknowledgement id we wish to send to the
     * channel.
     */
    public void enqueueAckId(String id) {
        ids.add(id);

        if (ids.size() >= queueSize) {
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
        channel.write(builder.build());
        ids.clear();
    }
}
