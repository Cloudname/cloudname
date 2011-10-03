package org.cloudname.timber.client;

import org.cloudname.log.pb.Timber;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

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

    private TimberClient() {}

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
        bootstrap.setPipelineFactory(new TimberClientPipelineFactory());

        // Make a new connection
        log.info("Client connecting to " + host + ":" + port + "...");
        ChannelFuture connectFuture =
            bootstrap.connect(new InetSocketAddress(host, port));

        // Wait for connection to succeed
        channel = connectFuture.awaitUninterruptibly().getChannel();
        log.info("Client connected to " + host + ":" + port);

        // Get the TimberClientHandler so we can use it to submit log
        // messages.
        handler = channel.getPipeline().get(TimberClientHandler.class);
    }

    /**
     * Shut down the client.
     */
    public void shutdown() {
        // do nothing for now
        // bootstrap.releaseExternalResources();
    }

    /**
     * Submit a Timber.LogEvent to the server.
     *
     * @param logEvent the Timber.LogEvent we wish to send to the server.
     */
    public void submitLogEvent(Timber.LogEvent logEvent) {
        handler.submitLogEvent(logEvent);
    }
}