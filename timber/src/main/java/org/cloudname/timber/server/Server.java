package org.cloudname.timber.server;

import org.cloudname.log.pb.Timber;
import org.cloudname.timber.common.Constants;
import org.cloudname.timber.server.handler.LogEventHandler;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * The Server class for the Timber log event server.
 *
 * @author borud
 */
public class Server {
    private static final Logger log = Logger.getLogger(Server.class.getName());


    // A ChannelGroup to keep track of all open channels
    static final ChannelGroup allChannels = new DefaultChannelGroup("timber-server");

    private int listenPort;
    private int dispatcherQueueLen = Constants.DEFAULT_DISPATCHER_QUEUE_LENGTH;
    private Dispatcher dispatcher;
    private ServerBootstrap bootstrap;
    private AtomicBoolean hasStarted = new AtomicBoolean(false);

    /**
     * Create server listening to default listenPort.
     */
    public Server() {
        this(Constants.DEFAULT_TIMBER_PORT);
    }

    /**
     * Create server listening to a specified port.
     *
     * @param listenPort the port that the server listens to for
     *   connections.
     */
    public Server(int listenPort) {
        this.listenPort = listenPort;
        dispatcher = new Dispatcher(dispatcherQueueLen);
    }

    /**
     * Add a log handler to the server.  This can only be done before
     * the server is started.
     *
     * @param handler the LogEventHandler we wish to add to the
     *   server.
     */
    public Server addHandler(LogEventHandler handler) {
        if (hasStarted.get()) {
            throw new IllegalStateException("Cannot add LogEventHandler after server started");
        }
        dispatcher.addHandler(handler);
        return this;
    }

    /**
     * Dispatch log message to log server.
     *
     * @param event a Timer.LogEvent to be dispatched to the log
     *   server.
     */
    public Server dispatch(Timber.LogEvent event) {
        dispatcher.dispatch(event);
        return this;
    }

    /**
     * Start the server.
     */
    public void start() {
        if (hasStarted.get()) {
            throw new IllegalStateException("Server already initialized");
        }
        hasStarted.set(true);

        bootstrap = new ServerBootstrap(
            new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool())
        );

        // Create and initialize dispatcher
        dispatcher.init();

        // Set up the event pipeline factory
        bootstrap.setPipelineFactory(new TimberServerPipelineFactory(dispatcher));
        log.info("Set up pipeline");

        // Set some socket options
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("backlog",
                            Constants.DEFAULT_TIMBER_SERVER_BACKLOG);
        bootstrap.setOption("child.receiveBufferSize",
                            Constants.DEFAULT_TIMBER_SERVER_RECEIVE_BUFFER_SIZE);


        // Bind port and start accepting incoming connections
        Channel channel = bootstrap.bind(new InetSocketAddress(listenPort));

        allChannels.add(channel);

        log.info("Timber listening to port " + listenPort);
    }

    /**
     * Shut down the server.
     */
    public void shutdown() {
        if (! hasStarted.get()) {
            throw new IllegalStateException("Server was not started");
        }
        ChannelGroupFuture future = allChannels.close();
        future.awaitUninterruptibly();
        bootstrap.releaseExternalResources();
        dispatcher.shutdown();
    }
}
