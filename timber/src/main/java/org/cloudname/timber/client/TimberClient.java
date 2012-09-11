package org.cloudname.timber.client;

import org.cloudname.log.pb.Timber;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Timber client.
 *
 * @author borud
 */
public class TimberClient {
    private static final Logger log = Logger.getLogger(TimberClient.class.getName());

    private final String host;
    private final int port;
    private ClientBootstrap bootstrap;
    private Channel channel;
    private volatile boolean wantShutdown = false;

    // Used to synchronize access on channel so that the channel
    // cannot change from under our feet when using it (channel is set
    // asynchronously on connection completion).
    private Object channelSync = new Object();
    private Set<AckEventListener> ackEventListeners = new HashSet<AckEventListener>();

    /**
     * Listens to Timber.AckEvent instances coming asynchronously from
     * the log server.  Note that this is called by a thread belonging
     * to the underlying IO machinery so AckEventListener
     * implementations have to be quick and they have to be thread
     * safe.
     *
     * @author borud
     */
    public static interface AckEventListener {
        /**
         * @param ackEvent a Timber.AckEvent.
         */
        public void ackEventReceived(Timber.AckEvent ackEvent);
    }

    /**
     * Create a Timber client.
     *
     * @param host the host where the log server runs.
     * @param port the port the log server listens to.
     */
    public TimberClient(final String host, final int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Start the client.
     */
    public void start() {
        bootstrap = new ClientBootstrap(
            new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool())
        );

        // Configure the bootstrap
        bootstrap.setPipelineFactory(new TimberClientPipelineFactory(this, bootstrap));
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("remoteAddress", new InetSocketAddress(host, port));

        // Make a new connection
        log.info("Client connecting to " + host + ":" + port + "...");
        bootstrap.connect().awaitUninterruptibly();
    }

    /**
     * Shut down the client.
     */
    public void shutdown() {
        wantShutdown = true;

        // The first step is always to get rid of any open channels.
        // If we do not the releaseExternalResources() method is just
        // going to hang until we do.
        synchronized(channelSync) {
            if ((channel != null) && channel.isConnected()) {
                try {
                    ChannelFuture closeFuture = channel.getCloseFuture();
                    channel.close();
                    closeFuture.await();
                } catch (InterruptedException e) {
                    // Swallow the exception
                    log.log(Level.WARNING, "Got exception during shutdown", e);
                }
            }

            // TODO(borud): Not sure if this should be inside or
            //   outside the synchronization barrier.
            try {
                ((TimberClientHandler)bootstrap.getPipelineFactory().getPipeline().getLast()).stopTimer();
            } catch (Exception e) {
                // Shouldn't happen with this factory
                e.printStackTrace();
            }
            bootstrap.releaseExternalResources();
        }
    }

    /**
     * This method is called by the TimberClientHandler.
     *
     * TODO(borud): factor out of public interface.
     */
    public void onAckEvent(Timber.AckEvent ack) {
        synchronized(ackEventListeners) {
            for (AckEventListener listener : ackEventListeners) {
                try {
                    listener.ackEventReceived(ack);
                } catch (Exception e) {
                    log.log(Level.WARNING, "AckEventListener " + listener + " threw an exception", e);
                }
            }
        }
    }

    /**
     * Callback method called by TimberClientHandler when the
     * connection has been made.  This can be the result of a connect
     * or a reconnect on connection loss.
     *
     * TODO(borud): factor out of public interface.
     *
     * @param channel the newly connected channel.
     */
    public void onConnect(Channel channel) {
        log.info("Client connected to " + host + ":" + port);
        synchronized(channelSync) {
            this.channel = channel;
        }
    }

    /**
     * Callback method called by TimberClientHandler when the
     * connection has been lost.
     *
     * TODO(borud): factor out of public interface.
     */
    public void onDisconnect() {
        log.info("DISCONNECTED from " + host + ":" + port);
        synchronized(channelSync) {
            channel = null;
        }
    }

    /**
     * Submit a Timber.LogEvent to the server.  If the underlying
     * connection to the log server is gone, this method will just
     * drop the LogEvent on the floor.
     *
     * @param logEvent the Timber.LogEvent we wish to send to the server.
     *
     * @return returns {@code true} if an attempt was made to write
     *   the log event. Returns {@code false} if there was no
     *   connection to the log server.
     */
    public boolean submitLogEvent(Timber.LogEvent logEvent) {
        // Note that the synchronization is necessary to make sure
        // that the channel does not disappear under our feet after
        // the conditional.
        synchronized(channelSync) {
            if (null == channel) {
                return false;
            }

            if (! channel.isConnected()) {
                return false;
            }

            if (wantShutdown) {
                return false;
            }

            channel.write(logEvent);
            return true;
        }
    }

    /**
     * Add an AckEventListener to this TimberClient.
     *
     * @param listener the AckEventListener we wish to register.
     * @return a {@code this} reference for method chaining.
     * @throws IllegalArgumentException if the listener has already been registered.
     */
    public TimberClient addAckEventListener(AckEventListener listener) {
        synchronized(ackEventListeners) {
            if (! ackEventListeners.add(listener)) {
                throw new IllegalArgumentException(
                    "This AckEventListener was already registered: " + listener.toString()
                );
            }
        }
        return this;
    }

    /**
     * Removes an AckEventListener from this TimberClient.  If the
     * AckEventListener was never registered we ignore this fact and
     * trundle on happily.
     *
     * @param listener the AckEventListener we wish to remove.
     * @return a {@code this} reference for method chaining.
     */
    public TimberClient removeAckEventListener(AckEventListener listener) {
        synchronized(ackEventListeners) {
            ackEventListeners.remove(listener);
        }
        return this;
    }

    /**
     * @return {@code true} if shutdown() has been called.
     */
    public boolean shutdownRequested() {
        return wantShutdown;
    }
}
