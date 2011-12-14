package org.cloudname.timber.client;

import org.cloudname.log.pb.Timber;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;

import java.util.concurrent.TimeUnit;
import java.net.InetSocketAddress;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 *
 *
 * @author borud
 */
public class TimberClientHandler extends SimpleChannelUpstreamHandler {
    private static final Logger log = Logger.getLogger(TimberClientHandler.class.getName());

    // Use common ReconnectDelayManager across all TimberClientHandler instances.
    private static final ReconnectDelayManager reconnectDelayManager  = new ReconnectDelayManager();

    // Initial delay before reconnect.  This is the delay used on the
    // first reconnect.  On successive reconnect attempts this value
    // is doubled up to RECONNECT_MAX_DELAY.
    private static final int RECONNECT_DELAY = 500;

    // The reconnect delay is doubled on each successive reconnect
    // failure, up to this value.
    private static final int RECONNECT_MAX_DELAY = 30000;

    // If the last reconnect was more than this number of seconds ago
    // we reset lastReconnectDelay to this value.
    private static final int RECONNECT_DELAY_RESET_TIME = 60000;

    // The time at which we last attempted a reconnect.
    private long lastReconnect = 0;

    // The last reconnect delay value we used.
    private int lastReconnectDelay = RECONNECT_DELAY;


    private final TimberClient client;
    private final ClientBootstrap bootstrap;
    private final Timer timer;

    /**
     * Create a Timber client handler.
     *
     * @param client the timber client.
     */
    public TimberClientHandler(TimberClient client, ClientBootstrap bootstrap) {
        // Do we need to call super()?  Since we extend SimpleChannelUpstreamHandler
        // it is likely the answer is yes.
        super();
        this.client = client;
        this.bootstrap = bootstrap;
        timer = new HashedWheelTimer();
    }

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent event) throws Exception {
        if (event instanceof ChannelStateEvent) {
            log.info(event.toString());
        }
        super.handleUpstream(ctx, event);
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

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent event) throws Exception {
        super.channelOpen(ctx, event);
        client.onConnect(event.getChannel());
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
        // Figure out how long we should delay the next reconnect attempt
        InetSocketAddress address = (InetSocketAddress) bootstrap.getOption("remoteAddress");
        int delay = reconnectDelayManager.getReconnectDelayForSocketAddress(address);

        // Set a timer that tries to reconnect.
        timer.newTimeout(new TimerTask() {
                public void run(Timeout timeout) throws Exception {
                    // There is no use in reconnecting if shutdown()
                    // has been called on the TimberClient
                    if (! client.shutdownRequested()) {
                        bootstrap.connect();
                    }
                }
            }, delay, TimeUnit.MILLISECONDS);

        // Alert the client that we have disconnected
        client.onDisconnect();
    }

    /**
     * Calculate the reconnection delay in milliseconds.
     */
    private synchronized int getReconnectDelay() {
        long now = System.currentTimeMillis();
        long diff = now - lastReconnectDelay;

        // If the last reconnect was long ago we reset lastReconnectDelay
        if (diff > RECONNECT_DELAY_RESET_TIME) {
            lastReconnectDelay = RECONNECT_DELAY;
            return lastReconnectDelay;
        }

        // Double the delay.
        lastReconnectDelay *= 2;

        // Make sure the delay is no longer than RECONNECT_MAX_DELAY
        if (lastReconnectDelay > RECONNECT_MAX_DELAY) {
            lastReconnectDelay = RECONNECT_MAX_DELAY;
        }

        return lastReconnectDelay;
    }
}