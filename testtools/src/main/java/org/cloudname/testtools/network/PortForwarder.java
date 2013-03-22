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

    private Thread portThread;
    private final Object threadMonitor = new Object();

    private final List<ClientThread> clientThreadList = new ArrayList<ClientThread>();
    private final AtomicBoolean pause = new AtomicBoolean(false);

    private final String hostName;
    private final int hostPort;

    /**
     * Constructor for port-forwarder. Does stat the forwarder.
     * @param myPort client port
     * @param hostName name of host to forward to.
     * @param hostPort port of host to forward to.
     * @throws IOException if unable to open server socket
     */
    public PortForwarder(final int myPort, final String hostName, final int hostPort) throws IOException {
        this.myPort = myPort;
        this.hostName = hostName;
        this.hostPort = hostPort;
        log.info("Starting port forwarder " + myPort + " -> " + hostPort);
        startServerSocketThread();
    }

    private void startServerSocketThread()
            throws IOException {
        openServerSocket();
        Runnable myRunnable = new Runnable() {
            @Override
            public void run()  {
                log.info("Forwarder running");
                while (isAlive.get() && !pause.get()) {
                    try {
                        final Socket clientSocket = serverSocket.accept();
                        synchronized (threadMonitor) {
                            if (isAlive.get() && !pause.get()) {
                                clientThreadList.add(new ClientThread(clientSocket, hostName, hostPort));
                            } else {
                                clientSocket.close();
                            }
                        }
                    } catch (IOException e) {
                        log.log(Level.SEVERE, "Got exception in forwarder", e);
                        // Keep going, maybe later connections will succeed.
                    }
                }
                log.info("Forwarder stopped");
            }
        };
        portThread = new Thread(myRunnable);
        // Make this a daemon thread, so it won't keep the VM running at shutdown.
        portThread.setDaemon(true);
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
     * @throws InterruptedException
     */
    public void pause() throws IOException, InterruptedException {
        final Thread currentServerThread;
        synchronized (threadMonitor) {
            if (!pause.compareAndSet(false, true)) {
                return;
            }
            for (ClientThread clientThread: clientThreadList) {
                clientThread.close();

            }
            clientThreadList.clear();
            serverSocket.close();
            /*
             * Make a copy of the server socket thread, so we can wait for it
             * to complete outside any monitor.
             */
            currentServerThread = portThread;
        }
        currentServerThread.join();
    }

    /**
     * Lets client start connecting again.
     * @throws IOException
     */
    public void unpause() throws IOException {
        synchronized (threadMonitor) {
            if (pause.compareAndSet(true, false)) {
                startServerSocketThread();
            }
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
        } catch (InterruptedException e) {
            log.severe("Interrupted while waiting for server thread to finish.");
            // Reassert interrupt.
            Thread.currentThread().interrupt();
        }
    }
}

