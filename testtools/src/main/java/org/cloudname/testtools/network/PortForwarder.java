package org.cloudname.testtools.network;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple class for setting up port forwarding in unit tests. This
 * enables killing the connection.  To restore the connection create a
 * new PortForwarder.
 *
 * TODO(borud): this class lacks unit tests
 *
 * @author dybdahl
 */
public class PortForwarder extends Thread {
    private final static Logger log = Logger.getLogger(PortForwarder.class.getName());

    private final int myPort;
    private final String hostName;
    private final int hostPort;
    private boolean isAlive = true;
    private ServerSocket serverSocket = null;
    private Socket clientSocket = null;
    private ClientThread clientThread;

    /**
     * TODO(borud): add javadoc
     */
    public PortForwarder(int myPort, String hostName, int hostPort) {
        this.myPort = myPort;
        this.hostName = hostName;
        this.hostPort = hostPort;
        log.info("Starting port forwarder " + myPort + "-> " + hostPort);
        start();
    }

    /**
     * TODO(borud): add javadoc.
     */
    public void run()  {

        try {
            serverSocket = new ServerSocket(myPort);
            log.info("Forwarder, server port running " + myPort);
            while (isAlive) {
                log.info("Now, taking clients to forwarder");
                clientSocket = serverSocket.accept();
                clientThread = new ClientThread(clientSocket, hostName, hostPort);
            }
        } catch (IOException e) {
            log.log(Level.SEVERE, "Got exception in forwarder", e);
            return;
        }
        log.info("Forwarder running");
    }

    /**
     * TODO(borud): add javadoc.
     */
    public synchronized  void disconnect() {
        log.info("Disconnect called");
        if (clientThread != null) {
            clientThread.disconnect();
        }
        clientThread = null;
    }

    /**
     * TODO(borud): add javadoc.
     */
    public void terminate() {
        isAlive = false;
        disconnect();
        synchronized (this) {
            if (clientSocket == null) {
                return;
            }

            try {
                clientSocket.close();
            } catch (IOException e) {
                log.log(Level.SEVERE, "Forwarder close client port failed, might be ok.", e);
            }
            try {
                serverSocket.close();
            } catch (IOException e) {
                log.log(Level.SEVERE, "Forwarder close server port failed, might be ok.", e);
            }
            clientSocket = null;
        }
    }
}

