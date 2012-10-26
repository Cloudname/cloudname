package org.cloudname.testtools.network;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TODO(borud): add Javadoc
 * TODO(borud): this class lacks unit tests
 *
 * @author dybdahl
 */
class ClientThread extends Thread {
    private final static Logger log = Logger.getLogger(ClientThread.class.getName());

    final private Socket clientSocket;
    private Socket serverSocket;
    final private String hostName;
    final private int hostPort;

    public String toString() {
        return hostName + " " + hostPort;
    }

    /**
     * TODO(borud): add Javadoc
     */
    public ClientThread(Socket clientSocket, String hostName, int hostPort) {
        this.clientSocket = clientSocket;
        this.hostName = hostName;
        this.hostPort = hostPort;
        start();
    }

    /**
     * TODO(borud): add Javadoc
     */
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
            log.severe("Portforwarder: Can not connect to " + hostName + ":" + hostPort);
            disconnect();
            return;
        }
        new ForwardThread(this, clientIn, serverOut);
        new ForwardThread(this, serverIn, clientOut);
    }

    /**
     * TODO(borud): add Javadoc
     */
    public synchronized void disconnect() {
        // TODO(borud): It would make sense to say what is being disconnected from what
        log.info("Disconnecting");
        try {
            serverSocket.close();
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error while closing server socket", e);
        }
        try {
            clientSocket.close();
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error while closing client socket", e);
        }
    }
}
