package org.cloudname.timber.client;

import org.cloudname.log.pb.Timber;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 *
 * @author borud
 */
public class TimberClientHandler extends SimpleChannelUpstreamHandler {
    private static final Logger log = Logger.getLogger(TimberClientHandler.class.getName());

    // Use common ReconnectDelayManager across all TimberClientHandler instances.
    private static final ReconnectDelayManager reconnectDelayManager  = new ReconnectDelayManager();

    private final TimberClient client;
    private final ClientBootstrap bootstrap;
    private final Timer timer;

    /**
     * Create a Timber client handler.
     *
     * @param client the timber client.
     */
    public TimberClientHandler(TimberClient client, ClientBootstrap bootstrap) {
        this.client = client;
        this.bootstrap = bootstrap;
        timer = new HashedWheelTimer();
    }

    public void stopTimer() {
        timer.stop();
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
        int delay = reconnectDelayManager.getReconnectDelayMs(address);


        // Don't even start a reconnect-task if we know we are shutting down
        if (! client.shutdownRequested()) {
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
        }
        // Alert the client that we have disconnected
        client.onDisconnect();
    }
}
