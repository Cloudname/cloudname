package org.cloudname.timber.server;

import org.cloudname.log.pb.Timber;
import org.jboss.netty.channel.Channel;

/**
 * Value class for transporting log events and the channel they came
 * from to the Dispatcher and on to the handlers.  This class is
 * immutable.
 *
 * @author borud
 */
public class LogEventQueueEntry {
    private final Timber.LogEvent event;
    private final Channel channel;

    /**
     * Create LogEventQueueEntry.
     *
     * @param event the logevent
     * @param channel the channel the logevent came from
     */
    public LogEventQueueEntry(final Timber.LogEvent event, final Channel channel) {
        if (null == event) {
            throw new NullPointerException("event cannot be null");
        }

        if (null == channel) {
            throw new NullPointerException("channel cannot be null");
        }

        this.event = event;
        this.channel = channel;
    }

    /**
     * Get the LogEvent.
     *
     * @return the LogEvent.
     */
    public Timber.LogEvent getLogEvent() {
        return event;
    }

    /**
     * Get the channel the LogEvent came from.
     *
     * @return the channel.
     */
    public Channel getChannel() {
        return channel;
    }
}
