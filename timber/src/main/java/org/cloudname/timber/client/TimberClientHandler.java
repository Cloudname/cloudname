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
    private final TimberClient client;
    private final ClientBootstrap bootstrap;
    private final ReconnectDelayManager reconnectDelayManager;
    private final Timer timer;

    /**
     * Create a Timber client handler.
     *
     * @param client the timber client.
     */
    public TimberClientHandler(TimberClient client, ClientBootstrap bootstrap, ReconnectDelayManager reconnectDelayManager) {
        this.client = client;
        this.bootstrap = bootstrap;
        this.reconnectDelayManager = reconnectDelayManager;
        timer = new HashedWheelTimer();
    }

    public void stopTimer() {
        timer.stop();
    }

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent event) throws Exception {
        if (event instanceof ChannelStateEvent) {
            logEntry(Level.INFO, event.toString(), null);
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
        logEntry(Level.INFO, "Got unknown response from log server " + event.toString(), null);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent exceptionEvent) {
        logEntry(Level.WARNING,
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

    // Note: use the logEntry method call to log; using this directly might cause deadlocks
    private static final Logger xlog = Logger.getLogger(TimberClientHandler.class.getName());
    /**
     * This method logs in a new thread and we're avoiding any deadlocks from exceptions and
     * downstream events that would otherwise happen. If one of the log handlers uses a lock we'd
     * normally allocate locks from the top down, ie. 1) Log handler, 2) Timber and 3) NIO libs but
     * if we get incoming data in the NIO libraries and an exception we'd get locks in this order:
     * 1) NIO 2) Log handler. If we launch logging in a new thread we'll get the log message a bit
     * later in the log handler but we'll avoid deadlocks.
     */
    private void logEntry(final Level logLevel, final String message, final Throwable cause) {
        (new Thread(new Runnable() {
            @Override
            public void run() {
                xlog.log(logLevel, message, cause);
            }
        })).start();
    }
}
