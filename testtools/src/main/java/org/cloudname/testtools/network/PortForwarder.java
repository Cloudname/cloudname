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

    private ClientThread clientThread;

    public PortForwarder(int myPort, String hostName, int hostPort) {
        this.myPort = myPort;
        this.hostName = hostName;
        this.hostPort = hostPort;
        start();
    }

    public void run()  {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(myPort);
            Socket clientSocket = serverSocket.accept();
            clientThread = new ClientThread(clientSocket, hostName, hostPort);
        } catch (IOException e) {
            return;
        }
    }

    public void disconnect() {
        clientThread.disconnect();
    }
}

class ClientThread extends Thread {
    final private Socket clientSocket;
    private Socket serverSocket;
    final private String hostName;
    final private int hostPort;

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
            System.err.println("Can not connect to " + hostName + ":" + hostPort);
            disconnect();
            return;
        }
        ForwardThread clientForward = new ForwardThread(this, clientIn, serverOut);
        ForwardThread serverForward = new ForwardThread(this, serverIn, clientOut);
    }


    public synchronized void disconnect() {
        try {
            serverSocket.close();
        } catch (Exception e) {}
        try {
            clientSocket.close();
        } catch (Exception e) {}
    }
}

class ForwardThread extends Thread {
    private static final int BUFFER_SIZE = 4096;
    InputStream inputStream;
    OutputStream outputStream;
    ClientThread parent;

    public ForwardThread(ClientThread aParent, InputStream aInputStream, OutputStream aOutputStream) {
        parent = aParent;
        inputStream = aInputStream;
        outputStream = aOutputStream;
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
        }
        // Notify parent thread that the connection is broken
        parent.disconnect();
    }
}