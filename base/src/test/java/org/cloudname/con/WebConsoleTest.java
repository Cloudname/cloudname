package org.cloudname.con;

import org.cloudname.con.servlet.SystemPropertiesServlet;

import org.cloudname.testtools.Net;
import org.cloudname.testtools.Http;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit test for the web console class.
 *
 * @author borud
 */
public class WebConsoleTest {

    /**
     * Perform simple test firing up a single WebConsole instance and
     * then taking it down.  Note that this uses a random port.
     */
    @Test
    public void testSimple() throws Exception {
        WebConsole.create(Net.getFreePort())
            .start()
            .waitUntilStarted()
            .shutdown();
    }

    /**
     * Make sure we catch extraneous start() calls and generate an
     * exception.  (Not using Junit "expected" annotation because we
     * want to do cleanup in a finally clause)
     */
    @Test
    public void testDoubleStart() throws Exception {
        boolean exceptionThrown = false;
        WebConsole con = WebConsole.create(Net.getFreePort());
            try {
                con.start();
                con.start(); // this one should fail
                fail();
            } catch (IllegalStateException e) {
                exceptionThrown = true;
            } finally {
                con.waitUntilStarted()
                    .shutdown();
            }

            assertTrue(exceptionThrown);
    }

    /**
     * This unit test fires up multiple instances of WebConsole in
     * parallel in a meek effort to figure out if it has problems
     * coexisting with other servers using the same underlying HTTP
     * server library.  You should only have one WebConsole instance
     * for a given application.
     */
    @Test
    public void testMultipleInstances() throws Exception {
        int numInstances = 5;
        WebConsole[] consoles = new WebConsole[numInstances];

        // Fire up numInstances WebConsole instances.
        for (int i = 0; i < numInstances; i++) {
            consoles[i] = WebConsole.create(Net.getFreePort()).start();
        }

        // Check that they are all up
        for (WebConsole con : consoles) {
            con.waitUntilStarted();
        }
        // TODO: Do some checks

        // Take them down again.
        for (WebConsole con : consoles) {
            con.shutdown();
        }
    }

    /**
     * Test adding a servlet.
     */
    @Test
    public void testAddServlet() throws Exception {
        WebConsole con = WebConsole.create(Net.getFreePort())
            .addServlet(new SystemPropertiesServlet(), "/xxpropz/*")
            .start()
            .waitUntilStarted();

        // Test servlet
        String s = Http.doGet("http://localhost:" + con.getPort() + "/xxpropz");
        assertNotNull(s);
        assertTrue(s.length() > 100);
        con.shutdown();
    }

    @Test
    public void accessSystemServlets() throws Exception {
        WebConsole con = WebConsole.create(Net.getFreePort())
            .start()
            .waitUntilStarted();

        String prefix = "http://localhost:" + con.getPort();
        assertNotNull(Http.doGet(prefix + "/varz"));
        assertNotNull(Http.doGet(prefix + "/propz"));
        assertNotNull(Http.doGet(prefix + "/"));
    }

    /**
     * Make sure we throw exception for null servlet
     */
    @Test (expected = NullPointerException.class)
    public void testAddServletWithNullServlet() throws Exception {
        WebConsole con = WebConsole.create(Net.getFreePort())
            .addServlet(null, "/failz/*");
    }

    /**
     * Make sure we throw exception for null path
     */
    @Test (expected = NullPointerException.class)
    public void testAddServletWithNullPath() throws Exception {
        WebConsole con = WebConsole.create(Net.getFreePort())
            .addServlet(new SystemPropertiesServlet(), null);
    }

    /**
     * Make sure we throw exception if user tries to register servlet
     * on root path
     */
    @Test (expected = IllegalArgumentException.class)
    public void testAddServletWithRootPath() throws Exception {
        WebConsole con = WebConsole.create(Net.getFreePort())
            .addServlet(new SystemPropertiesServlet(), "/");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testAddServletWithRootPathStar() throws Exception {
        WebConsole con = WebConsole.create(Net.getFreePort())
            .addServlet(new SystemPropertiesServlet(), "/*");
    }

    /**
     * Make sure we throw exception if user tries to map servlet to
     * path which already exists.
     */
    @Test (expected = IllegalStateException.class)
    public void testAddServletToExistingPath() throws Exception {
        WebConsole con = WebConsole.create(Net.getFreePort())
            .addServlet(new SystemPropertiesServlet(), "/foo")
            .addServlet(new SystemPropertiesServlet(), "/foo");
    }
}