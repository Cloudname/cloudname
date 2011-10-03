package org.cloudname.timber.server;

import java.net.ServerSocket;
import java.net.SocketException;
import java.io.IOException;

import org.junit.*;
import static org.junit.Assert.*;

public class ServerTest {

    public static int getFreePort() throws IOException {
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(0);
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        } finally {
            if (null != ss) {
                ss.close();
            }
        }
    }

    @Test
    public void testServerSimple() throws Exception
    {
        Server server = new Server(getFreePort());
        server.start();
        server.shutdown();
    }
}