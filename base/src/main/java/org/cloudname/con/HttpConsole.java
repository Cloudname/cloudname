package org.cloudname.con;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Executors;

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

    public static final int DEFAULT_PORT = 5601;
    
    public static void main(String[] args) {
        HttpConsole.create(DEFAULT_PORT);
    }

    private HttpConsole(int port) {
        this.port = port;
        setupDefaultWidgets();

        bootstrap = new ServerBootstrap(
            new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool())
        );

        bootstrap.setPipelineFactory(new HttpConsolePipelineFactory(this));

        try {
            channel = bootstrap.bind(new InetSocketAddress(port));
        } catch (ChannelException e) {
            throw new RuntimeException(e);
        }
    }

    private void setupDefaultWidgets() {
        addWidget(new RootWidget(), "/");
        addWidget(new MonitorWidget(), "/varz/*");
        addWidget(new SystemPropertiesWidget(), "/propz/*");
    }

    public HttpConsole addWidget(HttpWidget widget, String path) {
        if(path==null) throw new NullPointerException("Path is NULL");
        if(widget==null) throw new NullPointerException("Widget is NULL");
        if(path.equals("/") || path.equals("/*")) {
            if(!(widget instanceof RootWidget)) {
                throw new IllegalArgumentException("You are not allowed to add Widget to root path");                
            }
        }
        if(pathToWidgetMap.containsKey(path)) {
            throw new IllegalStateException("Path already taken");
        }
        
        // Invoke init() only once, even if Widget is added on several paths
        if(!pathToWidgetMap.containsValue(widget)) {
            widget.init(this);
        }
        pathToWidgetMap.put(path, widget);
        return this;
    }

    public static HttpConsole create(int port) {
        return new HttpConsole(port);
    }

    public void shutdown() {
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
     * @param requestPath
     * @return List widgets matching the requestPath
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

    public Set<String> getWidgetPaths() {
        return pathToWidgetMap.keySet();
    }

    public int getPort() {
        return port;
    }

}
