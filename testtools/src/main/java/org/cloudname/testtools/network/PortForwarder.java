package org.cloudname.testtools.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple class for setting up port forwarding in unit tests. This
 * enables killing the connection.
 *
 * TODO(borud): this class lacks unit tests
 *
 * @author dybdahl
 */
public class PortForwarder {
    private final static Logger log = Logger.getLogger(PortForwarder.class.getName());

    private final int myPort;
    private final AtomicBoolean isAlive = new AtomicBoolean(true);
    private ServerSocket serverSocket = null;

    private final Thread portThread;
    private final Object threadMonitor = new Object();

    private final List<ClientThread> clientThreadList = new ArrayList<ClientThread>();
    private final AtomicBoolean pause = new AtomicBoolean(false);

    /**
     * Constructor for port-forwarder. Does stat the forwarder.
     * @param myPort client port
     * @param hostName name of host to forward to.
     * @param hostPort port of host to forward to.
     * @throws IOException if unable to open server socket
     */
    public PortForwarder(final int myPort, final String hostName, final int hostPort) throws IOException {
        this.myPort = myPort;
        log.info("Starting port forwarder " + myPort + " -> " + hostPort);

        openServerSocket();
        Runnable myRunnable = new Runnable() {
            @Override
            public void run()  {
                log.info("Forwarder running");
                while (isAlive.get()) {
                    if (pause.get()) {
                        continue;
                    }
                    try {
                        final Socket clientSocket = serverSocket.accept();
                        synchronized (threadMonitor) {
                            if (isAlive.get() && !pause.get()) {
                                clientThreadList.add(new ClientThread(clientSocket, hostName, hostPort));
                            }
                        }
                    } catch (IOException e) {
                        if (pause.get()) {
                            // Keep quiet, IOExceptions are to be expected if paused during accept()
                        } else {
                            log.log(Level.SEVERE, "Got exception in forwarder", e);
                            // Keep going, maybe later connections will succeed.
                        }
                    }

                }
            }

        };
        portThread = new Thread(myRunnable);
        portThread.start();
    }

    private void openServerSocket() throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress("localhost", myPort));
    }

    /**
     * Forces client to loose connection and refuses to create new (closing attempts to connect).
     * @throws IOException
     */
    public void pause() throws IOException {
        synchronized (threadMonitor) {
            pause.set(true);
            for (ClientThread clientThread: clientThreadList) {
                clientThread.close();

            }
            clientThreadList.clear();
            serverSocket.close();
        }
    }

    /**
     * Lets client start connecting again.
     * @throws IOException
     */
    public void unpause() throws IOException {
        synchronized (threadMonitor) {
            pause.set(false);
            openServerSocket();
        }
    }

    /**
     * Shuts down the forwarder.
     */
    public void close() {
        isAlive.set(false);
        try {
            pause();
        } catch (final IOException e) {
            // Ignore this
            log.severe("Could not close server socket.");
        }
    }
}

