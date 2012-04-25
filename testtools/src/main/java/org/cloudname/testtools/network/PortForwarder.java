package org.cloudname.testtools.network;

import java.io.*;
import java.net.*;

/**
 * Simple class for setting up port forwarding in unit tests. This enables killing the connection.
 * To restore the connection create a new PortForwarder.
 */
public class PortForwarder extends Thread {
    
    final private int myPort;
    final private String hostName;
    final private int hostPort;
    boolean isAlive = true;

    private ClientThread clientThread;

    public PortForwarder(int myPort, String hostName, int hostPort) {
        this.myPort = myPort;
        this.hostName = hostName;
        this.hostPort = hostPort;
        System.out.println("Starting port forwarder " + myPort + "-> " + hostPort);
        start();
    }
    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    public void run()  {

        try {
            serverSocket = new ServerSocket(myPort);
            System.out.println("Forwarder, server port running " + myPort);
            while (isAlive) {
                System.out.println("Now, taking clients to forwarder");
                clientSocket = serverSocket.accept();
                clientThread = new ClientThread(clientSocket, hostName, hostPort);
            }
        } catch (IOException e) {
            System.err.println("Problems in forwarder " + e.getMessage() + e.getStackTrace());
            return;
        }
        System.out.println("Forwarder running");
    }

    public synchronized  void disconnect() {
        System.out.println("Someone called disconnect");
        if (clientThread != null) clientThread.disconnect();
        clientThread = null;
    }

    public void terminate() {
        isAlive = false;
        disconnect();
        synchronized (this) {
            if (clientSocket == null) return;
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.out.println("Forwarder close client port failed, might be ok."+ e.getMessage());
            }
            try {
                serverSocket.close();
            } catch (IOException e) {
                System.out.println("Forwarder close server port failed, might be ok." + e.getMessage());
            }
            clientSocket = null;
        }
    }
}

class ClientThread extends Thread {
    final private Socket clientSocket;
    private Socket serverSocket;
    final private String hostName;
    final private int hostPort;

    public String toString() {
        return hostName + " " + hostPort;
    }
    
    public ClientThread(Socket clientSocket, String hostName, int hostPort) {
        this.clientSocket = clientSocket;
        this.hostName = hostName;
        this.hostPort = hostPort;
        start();
    }



    public void run() {
        InputStream clientIn, serverIn;
        OutputStream clientOut, serverOut;

        try {
            serverSocket = new Socket(hostName, hostPort);
            clientIn = clientSocket.getInputStream();
            clientOut = clientSocket.getOutputStream();
            serverIn = serverSocket.getInputStream();
            serverOut = serverSocket.getOutputStream();
        } catch (IOException ioe) {
            System.err.println("Portforwarder: Can not connect to " + hostName + ":" + hostPort);
            disconnect();
            return;
        }
        new ForwardThread(this, clientIn, serverOut);
        new ForwardThread(this, serverIn, clientOut);
    }


    public synchronized void disconnect() {
        System.out.println("Disconnecting");
        try {
            serverSocket.close();
        } catch (Exception e) {
            System.err.println("Disconnect fun: " + e.getMessage() +  e.getStackTrace());
        }
        try {
            clientSocket.close();
        } catch (Exception e) {
            System.err.println("Disconnect fun problems: " + e.getMessage() +  e.getStackTrace());
        }
    }
}

class ForwardThread extends Thread {
    private static final int BUFFER_SIZE = 4096;
    InputStream inputStream;
    OutputStream outputStream;
    ClientThread parent;

    public ForwardThread(ClientThread parent, InputStream inputStream, OutputStream outputStream) {
        this.parent = parent;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        start();
    }

    public void run() {
        byte[] buffer = new byte[BUFFER_SIZE];
        try {
            while (true) {
                int bytesRead = inputStream.read(buffer);

                if (bytesRead == -1)
                    break; // End of stream is reached --> exit

                outputStream.write(buffer, 0, bytesRead);
                outputStream.flush();
            }
        } catch (IOException e) {
            // Read/write failed --> connection is broken
            System.out.println("Forwarding in loop died.." + parent.toString() + e.getMessage());
        }
        // Notify parent thread that the connection is broken
        parent.disconnect();
    }
}