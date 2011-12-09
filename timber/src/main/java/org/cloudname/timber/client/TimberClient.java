package org.cloudname.timber.client;

import org.cloudname.log.pb.Timber;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.util.Set;
import java.util.HashSet;

import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Timber client.
 *
 * @author borud
 */
public class TimberClient {
    private static final Logger log = Logger.getLogger(TimberClient.class.getName());

    private String host;
    private int port;

    private TimberClientHandler handler;
    private ClientBootstrap bootstrap;
    private Channel channel;

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


    public TimberClient(String host, int port) {
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

        // Configure the pipeline factory
        bootstrap.setPipelineFactory(new TimberClientPipelineFactory(this));

        // Make a new connection
        log.info("Client connecting to " + host + ":" + port + "...");
        ChannelFuture connectFuture =
            bootstrap.connect(new InetSocketAddress(host, port));

        // Wait for connection to succeed
        channel = connectFuture.awaitUninterruptibly().getChannel();
        log.info("Client connected to " + host + ":" + port);
    }

    /**
     * Shut down the client.
     */
    public void shutdown() {
        // The first step is always to get rid of any open channels.
        // If we do not the releaseExternalResources() method is just
        // going to hang until we do.
        if ((channel != null) && channel.isOpen()) {
            try {
                ChannelFuture closeFuture = channel.getCloseFuture();
                channel.close();
                closeFuture.await();
            } catch (InterruptedException e) {
                // TODO(borud): is there anything else we can do at this point?
                throw new RuntimeException(e);
            }
        }

        bootstrap.releaseExternalResources();
    }

    /**
     * This method is called by the TimberClient
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
     * Submit a Timber.LogEvent to the server.
     *
     * @param logEvent the Timber.LogEvent we wish to send to the server.
     */
    public void submitLogEvent(Timber.LogEvent logEvent) {
            channel.write(logEvent);
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
}
