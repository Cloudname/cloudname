package org.cloudname.codelabs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.cloudname.*;
import org.cloudname.testtools.Net;
import org.cloudname.CoordinateException;
import org.cloudname.EndpointException;
import org.cloudname.zk.ZkCloudname;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * A class that has a web server responding to /info. It has a instance number that it publishes.
 * @author dybdahl
 */
public class ServerExampleCloudname {
    private int port;
    private int instance;

    /**
     * Constructor
     * @param instance number for this instance.
     */
    ServerExampleCloudname(int instance) {
        this.instance = instance;
    }

    /**
     * Handler for HTTP requests on /info.
     */
    class InfoHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            InputStream is = t.getRequestBody();
            String response = String.format("Port %s, instance %s", Integer.toString(port), Integer.toString(instance));
            t.sendResponseHeaders(200, response.length());
            OutputStream outputStream = t.getResponseBody();
            outputStream.write(response.getBytes());
            outputStream.close();
        }
    }

    /**
     * Method to set-up and start the web server.
     * @throws IOException
     */
    public  void runServer() throws IOException, CloudnameException, CoordinateException, EndpointException {
        port = Net.getFreePort();
        System.err.println("I think that port " + Integer.toString(port) + " is free and will use it.");
        Cloudname cloudName = new ZkCloudname.Builder().setConnectString("127.0.0.1:5454").build().connect();
        Coordinate coordinate = Coordinate.parse(String.format("%s.hello.somebody.aa", instance));
        ServiceHandle handle = cloudName.claim(coordinate);
        Endpoint endpoint = new Endpoint(coordinate, "info", "127.0.0.1", port, "http", null);
        handle.putEndpoint(endpoint);
        handle.setStatus(new ServiceStatus(ServiceState.RUNNING, "I am alive and kicking."));
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 45 /*backlog*/);
        server.createContext("/info", new InfoHandler());
        server.setExecutor(null);
        server.start();
    }

    /**
     * @param args The first and only argument is the instance number
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, CloudnameException, EndpointException, CoordinateException {
        ServerExampleCloudname server = new ServerExampleCloudname(Integer.parseInt(args[0]));
        server.runServer();
    }
}
