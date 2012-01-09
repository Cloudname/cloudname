package org.cloudname.codelabs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.cloudname.testtools.Net;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;


public class ServerExample {
    private int port;

    class InfoHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            InputStream is = t.getRequestBody();
            String response = String.format("I am serving from port %s.", Integer.toString(port));
            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

    public  void runServer() throws IOException {
        port = Net.getFreePort();
        System.err.println("I think that port " + Integer.toString(port) + " is free and will use it.");
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 41 /*backlog*/);
        server.createContext("/info", new InfoHandler());
        server.setExecutor(null);
        server.start();
    }

    public static void main(String[] args) throws IOException {
        ServerExample server = new ServerExample();
        server.runServer();
    }
}
