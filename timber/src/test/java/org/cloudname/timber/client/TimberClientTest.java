package org.cloudname.timber.client;

import org.cloudname.timber.server.Server;

import org.cloudname.testtools.Net;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for TimberClient class.
 *
 * @author borud
 */
public class TimberClientTest {
    private Server server;
    private int port;

    @Before
    public void setUp() throws Exception {
        port = Net.getFreePort();
        server = new Server(port);
        server.start();
    }

    @After
    public void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    public void testSimple() throws Exception {
        TimberClient client = new TimberClient("localhost", port);
        client.start();
        client.shutdown();
    }
}