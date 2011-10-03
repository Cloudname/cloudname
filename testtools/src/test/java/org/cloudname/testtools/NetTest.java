package org.cloudname.testtools;

import java.util.List;
import java.util.ArrayList;
import java.net.ServerSocket;
import java.io.IOException;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit test for Net class.
 *
 * @author borud
 */
public class NetTest {

    /**
     * Try getting a free port 10 times in a row and binding them.
     * This test may exhibit flakeyness since what we are doing is
     * inherently unsound. :-)
     */
    @Test public void testGetFreePort() throws IOException {
        int numIterations = 10;
        List<ServerSocket> sockets = new ArrayList<ServerSocket>(numIterations);

        // Get the ports and bind to them
        for (int i = 0; i < numIterations; i++) {
            int port = Net.getFreePort();
            ServerSocket socket = new ServerSocket(port);
            socket.setReuseAddress(true);
            sockets.add(socket);
        }

        // Now dispose of the sockets
        for (ServerSocket socket : sockets) {
            socket.close();
        }
    }

    /**
     * Try getting multiple ports in several iterations and ensure
     * that they are unique by ServerSocket instances listening to all
     * of the ports allocated.
     */
    @Test public void testGetFreePorts() throws IOException {
        int numIterations = 5;
        int numPorts = 10;
        List<ServerSocket> sockets = new ArrayList<ServerSocket>(numPorts * numIterations);

        // Get numPorts numIterations times
        for (int i = 0; i < numIterations; i++) {
            int[] ports = Net.getFreePorts(numPorts);

            // Allocate and create server sockets
            for (int port : ports) {
                ServerSocket socket = new ServerSocket(port);
                assertNotNull(socket);
                socket.setReuseAddress(true);
                sockets.add(socket);
            }
        }

        // Close down the sockets
        for (ServerSocket socket : sockets) {
            socket.close();
        }
    }
}