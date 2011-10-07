package org.cloudname.con;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.Collections;

import org.cloudname.con.netty.HttpConsolePipelineFactory;
import org.cloudname.con.widget.HttpWidget;
import org.cloudname.con.widget.MonitorWidget;
import org.cloudname.con.widget.RootWidget;
import org.cloudname.con.widget.SystemPropertiesWidget;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 *
 * Entry class for the HttpConsole
 *
 * Please see org.cloudname.example.Main for example usage
 *
 * @author paulrene
 *
 */
public class HttpConsole {
    private int port;
    private Channel channel;
    private ServerBootstrap bootstrap;

    private HashMap<String, HttpWidget> pathToWidgetMap = new HashMap<String, HttpWidget>();

    /**
     * Create HttpConsole instance which listens to {@code port}.
     *
     * @param port the port we wish the HttpConsole to listen to.
     */
    private HttpConsole(int port) {
        this.port = port;
    }

    /**
     * Create a new HttpConsole instance which listens to {@code port}.
     */
    public static HttpConsole create(int port) {
        return new HttpConsole(port);
    }

    /**
     * Populate the HttpConsole with the default system widgets and
     * start the server.
     */
    public HttpConsole start() {
        // Setup default widgets
        addWidget(new RootWidget(), "/");
        addWidget(new MonitorWidget(), "/varz/*");
        addWidget(new SystemPropertiesWidget(), "/propz/*");

        bootstrap = new ServerBootstrap(
            new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool())
        );

        bootstrap.setPipelineFactory(new HttpConsolePipelineFactory(this));

        try {
            channel = bootstrap.bind(new InetSocketAddress(port));
        } catch (ChannelException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    /**
     * Add a new HttpWidget to the given path and initialize it
     * 
     * @param widget Widget to add to the path
     * @param path The path to add the Widget to. You may use the wildcard at the end of the path, eg. "/hello/*"
     * @throws NullPointerException Will be thrown if widget or path is null.
     * @throws IllegalArgumentException Will be thrown if you try to add a Widget to the root path
     * @throws IllegalStateException Will be thrown if you try to add a Widget to a path that is already taken
     * 
     */
    public HttpConsole addWidget(HttpWidget widget, String path) {
        if (null == path) {
            throw new NullPointerException("Path is NULL");
        }

        if (widget==null) {
            throw new NullPointerException("Widget is NULL");
        }

        if (path.equals("/") || path.equals("/*")) {
            if (!(widget instanceof RootWidget)) {
                throw new IllegalArgumentException("You are not allowed to add Widget to root path");
            }
        }

        if (pathToWidgetMap.containsKey(path)) {
            throw new IllegalStateException("Path already taken");
        }

        // Invoke init() only once, even if Widget is added on several paths
        if (!pathToWidgetMap.containsValue(widget)) {
            widget.init(this);
        }
        pathToWidgetMap.put(path, widget);
        return this;
    }


    /**
     * 
     * Unbinds the connector and releases resources.
     * 
     * This will also invoke destroy() on all registered Widgets
     * 
     */
    public void shutdown() {
        if (null == channel) {
            throw new IllegalStateException("HttpConsole was never start()'ed");
        }

        channel.close();

        for(HttpWidget widget : pathToWidgetMap.values()) {
            try {
                widget.destroy();
            } catch (RuntimeException e) {
                // TODO
            }
        }
    }

    /**
     * @return the port the HttpConsole is listening to.
     */
    public int getPort() {
        return port;
    }

    
    /**
     * @return The registered paths
     */
    public Set<String> getWidgetPaths() {
        return Collections.unmodifiableSet(pathToWidgetMap.keySet());
    }

    /**
     * @param requestPath the path requested by the client.
     * @return List widgets matching the requestPath.
     */
    public ArrayList<HttpWidget> getMatchingWidgets(String requestPath) {
        ArrayList<HttpWidget> widgetList = new ArrayList<HttpWidget>();

        for (String path : pathToWidgetMap.keySet()) {
            if (path.endsWith("/*")) {
                if(requestPath.startsWith(path.substring(0, path.length()-2))) {
                    widgetList.add(pathToWidgetMap.get(path));
                }
            } else if (requestPath.equals(path)) {
                widgetList.add(pathToWidgetMap.get(path));
            }
        }

        return widgetList;
    }
}
