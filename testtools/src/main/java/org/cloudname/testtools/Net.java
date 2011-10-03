package org.cloudname.testtools;

import java.util.List;
import java.util.ArrayList;

import java.net.ServerSocket;
import java.net.SocketException;
import java.io.IOException;

/**
 * Testing utilities for networking.
 *
 * @author borud
 */
public class Net {
    /**
     * Find a network port that is not in use.
     * <p>
     * The only way to implement this sensibly without obvious race
     * conditions would be if we could return an opened listen socket.
     * This way we would only run into trouble if we were unable to
     * find a port we can bind at all.
     * <p>
     * However, since most of the code we need to test doesn't let us
     * inject listen sockets this is impractical.  We could of course
     * pass a socket back and have the client code close it and then
     * re-use it, but that would be burdening the developer unduly.
     * <p>
     * This means that the goal of this code is to, with some
     * probability, locate a port number that appears to be free and
     * hope that the time window is narrow enough so other threads or
     * processes cannot grab it before we make use of it.
     * @throws IOException if an IO error occurs
     * @return a port number which is probably free so we can bind it
     */
    public static int getFreePort() throws IOException {
        int[] port = getFreePorts(1);
        return port[0];
    }

    /**
     * Get multiple ports.  This is useful when you need more than one
     * port number before you start binding any of the ports.
     *
     * @param numPorts the number of port numbers we need.
     * @return an array of numPorts port numbers.
     * @throws IOException if an IO error occurs.
     */
    public static int[] getFreePorts(int numPorts) throws IOException {
        List<ServerSocket> sockets = new ArrayList<ServerSocket>(numPorts);
        int[] portNums = new int[numPorts];

        try {
            for (int i = 0; i < numPorts; i++) {
                // Calling the constructor of ServerSocket with the port
                // number argument set to zero has defined semantics: it
                // allocates a free port.
                ServerSocket ss = new ServerSocket(0);
                ss.setReuseAddress(true);
                sockets.add(ss);
                portNums[i] = ss.getLocalPort();
            }
            return portNums;
        } finally {
            for (ServerSocket sock : sockets) {
                sock.close();
            }
        }
    }
}