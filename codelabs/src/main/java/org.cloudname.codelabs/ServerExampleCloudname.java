package org.cloudname.codelabs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.cloudname.*;
import org.cloudname.testtools.Net;
import org.cloudname.zk.ZkCloudname;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;


public class ServerExampleCloudname {
    private int port;
    private int instance;

    ServerExampleCloudname(int instance) {
        this.instance = instance;
    }

    class InfoHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            InputStream is = t.getRequestBody();
            String response = String.format("I am serving from port %s. I am instance %s", Integer.toString(port),
                    Integer.toString(instance));
            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

    public  void runServer() throws IOException {
        port = Net.getFreePort();
        Cloudname cloudName = new ZkCloudname.Builder().setConnectString("127.0.0.1:5454").build().connect();
        Coordinate coordinate = Coordinate.parse(String.format("%s.hello-word.somebody.aa", instance));
        ServiceHandle handle = cloudName.claim(coordinate);
        Endpoint endpoint = new Endpoint(coordinate, "info", "localhost", port, "http", null);
        handle.putEndpoint(endpoint);
        handle.setStatus(new ServiceStatus(ServiceState.RUNNING, "I am alive."));
        System.err.println("I think that port " + Integer.toString(port) + " is free and will use it.");
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 45 /*backlog*/);
        server.createContext("/info", new InfoHandler());
        server.setExecutor(null);
        server.start();
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Please specify which instance I am (int)");
            return;
        }
        ServerExampleCloudname server = new ServerExampleCloudname(Integer.parseInt(args[0]));
        server.runServer();
    }
}
