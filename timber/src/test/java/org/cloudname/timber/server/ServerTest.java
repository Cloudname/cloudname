package org.cloudname.timber.server;

import org.cloudname.testtools.Net;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for Server class.
 *
 * @author borud
 */
public class ServerTest {
    @Test
    public void testServerSimple() throws Exception
    {
        Server server = new Server(Net.getFreePort());
        server.start();
        server.shutdown();
    }
}