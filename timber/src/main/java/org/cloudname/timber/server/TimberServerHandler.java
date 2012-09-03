package org.cloudname.timber.server;

import org.cloudname.log.pb.Timber;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Handler for incoming log messages.
 *
 * @author borud
 */
public class TimberServerHandler
    extends SimpleChannelUpstreamHandler
{
    private static final Logger log = Logger.getLogger(TimberServerHandler.class.getName());
    private final Dispatcher dispatcher;

    public TimberServerHandler(Dispatcher dispatcher)
    {
        this.dispatcher = dispatcher;
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) {
        Server.allChannels.add(e.getChannel());
    }

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent event)
        throws Exception
    {
        if (event instanceof ChannelStateEvent) {
            log.info("ChannelStateEvent >>> " + event.toString());
        }
        super.handleUpstream(ctx, event);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event)
    {
        Timber.LogEvent logEvent = (Timber.LogEvent) event.getMessage();
        dispatcher.dispatch(logEvent, ctx.getChannel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event)
    {
        log.log(Level.WARNING,
                "Exception from downstream",
                event.getCause());
        event.getChannel().close();
    }
}
