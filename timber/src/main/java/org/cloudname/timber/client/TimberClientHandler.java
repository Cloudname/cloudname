package org.cloudname.timber.client;

import org.cloudname.log.pb.Timber;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import java.util.logging.Logger;
import java.util.logging.Level;

/**
 *
 *
 * @author borud
 */
public class TimberClientHandler extends SimpleChannelUpstreamHandler {
    private static final Logger log = Logger.getLogger(TimberClientHandler.class.getName());

    private volatile Channel channel;
    private TimberClient client;

    /**
     * Create a Timber client handler.
     *
     * @param client the timber client.
     */
    public TimberClientHandler(TimberClient client) {
        // Do we need to call super()?  Since we extend SimpleChannelUpstreamHandler
        // it is likely the answer is yes.
        super();
        this.client = client;
    }

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent event) throws Exception {
        if (event instanceof ChannelStateEvent) {
            log.info(event.toString());
        }
        super.handleUpstream(ctx, event);
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent event) throws Exception {
        channel = event.getChannel();
        super.channelOpen(ctx, event);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, final MessageEvent event) {
        Object obj = event.getMessage();

        // When we get an AckEvent we dispatch it back to the TimberClient.
        if (obj instanceof Timber.AckEvent) {
            Timber.AckEvent ackEvent = (Timber.AckEvent) obj;
            client.onAckEvent(ackEvent);
            return;
        }

        // If we get something else we log it.
        log.info("Got unknown response from log server " + event.toString());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent exceptionEvent) {
        log.log(Level.WARNING,
                "Exception from downstream",
                exceptionEvent.getCause());
        exceptionEvent.getChannel().close();
    }
}