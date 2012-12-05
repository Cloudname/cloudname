package org.cloudname.testtools.network;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ClientThread forwards communication for one pair of sockets.
 * TODO(borud): this class lacks unit tests
 *
 * @author dybdahl
 */
class ClientThread  {
    private final static Logger log = Logger.getLogger(ClientThread.class.getName());

    private Socket serverSocket = null;
    private Socket clientSocket = null;
    private Object threadMonitor = new Object();


    /**
     * Constructor
     * @param clientSocket socket crated for incomming call
     * @param hostName destination host name
     * @param hostPort destination host port
     */
    public ClientThread(final Socket clientSocket, final String hostName, final int hostPort) {
        this.clientSocket = clientSocket;
        Runnable myRunnable = new Runnable() {
            @Override
            public void run() {
                final InputStream clientIn, serverIn;
                final OutputStream clientOut, serverOut;

                try {
                    synchronized (threadMonitor) {
                        serverSocket = new Socket(hostName, hostPort);
                    }
                    clientIn = clientSocket.getInputStream();
                    clientOut = clientSocket.getOutputStream();
                    serverIn = serverSocket.getInputStream();
                    serverOut = serverSocket.getOutputStream();
                } catch (IOException ioe) {
                    log.severe("Portforwarder: Can not connect to " + hostName + ":" + hostPort);
                    try {
                        if (serverSocket != null) {
                            serverSocket.close();
                        }
                    } catch (IOException e) {
                        log.severe("Could not close server socket");
                    }
                    return;
                }
                synchronized (threadMonitor) {
                    startForwarderThread(clientIn, serverOut);
                    startForwarderThread(serverIn, clientOut);
                }
            }
        };
        Thread fireAndForget = new Thread(myRunnable);
        fireAndForget.start();
    }

    /**
     * Closes sockets, which again closes the running threads.
     */
    public void close() {
        synchronized (threadMonitor) {
            try {
                if (serverSocket != null) {
                    serverSocket.close();
                    serverSocket = null;
                }
            } catch (Exception e) {
                log.log(Level.SEVERE, "Error while closing server socket", e);
            }
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                    clientSocket = null;
                }
            } catch (Exception e) {
                log.log(Level.SEVERE, "Error while closing client socket", e);
            }
        }
    }

    private Thread startForwarderThread(
            final InputStream inputStream, final OutputStream outputStream) {
        final int BUFFER_SIZE = 4096;
        Runnable myRunnable = new Runnable() {
            @Override
            public void run() {
                byte[] buffer = new byte[BUFFER_SIZE];
                try {
                    while (true) {
                        int bytesRead = inputStream.read(buffer);

                        if (bytesRead == -1)
                            // End of stream is reached --> exit
                            break;

                        outputStream.write(buffer, 0, bytesRead);
                        outputStream.flush();
                    }
                } catch (IOException e) {
                    // Read/write failed --> connection is broken
                    log.log(Level.SEVERE, "Forwarding in loop died.");
                }
                // Notify parent thread that the connection is broken
                close();
            }
        };
        Thread forwarder = new Thread(myRunnable);
        forwarder.start();
        return forwarder;
    }
}
