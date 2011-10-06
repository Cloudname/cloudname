package org.cloudname.con;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.cloudname.con.HttpConsole;
import org.cloudname.con.widget.SystemPropertiesWidget;
import org.cloudname.testtools.Http;
import org.cloudname.testtools.Net;
import org.junit.Test;

/**
 * Unit test for the web console class.
 *
 * @author borud
 * @author paulrene
 */
public class HttpConsoleTest {

    /**
     * Perform simple test firing up a single HttpConsole instance and
     * then taking it down.  Note that this uses a random port.
     */
    @Test
    public void testSimple() throws Exception {
        HttpConsole
            .create(Net.getFreePort())
            .start()
            .shutdown();
    }

    /**
     * This unit test fires up multiple instances of HttpConsole in
     * parallel in a meek effort to figure out if it has problems
     * coexisting with other servers using the same underlying HTTP
     * server library.  You should only have one HttpConsole instance
     * for a given application.
     */
    @Test
    public void testMultipleInstances() throws Exception {
        int numInstances = 5;
        HttpConsole[] consoles = new HttpConsole[numInstances];

        // Fire up numInstances WebConsole instances.
        for (int i = 0; i < numInstances; i++) {
            consoles[i] = HttpConsole
                .create(Net.getFreePort())
                .start();
        }

        // TODO: Do some checks

        // Take them down again.
        for (HttpConsole con : consoles) {
            con.shutdown();
        }
    }

    /**
     * Test adding a widget.
     */
    @Test
    public void testAddWidget() throws Exception {
        HttpConsole con = HttpConsole
            .create(Net.getFreePort())
            .start()
            .addWidget(new SystemPropertiesWidget(), "/xxpropz/*");

        // Test widget
        String s = Http.doGet("http://localhost:" + con.getPort() + "/xxpropz");
        assertNotNull(s);
        assertTrue(s.length() > 100);
        con.shutdown();
    }

    @Test
    public void accessSystemWidgets() throws Exception {
        HttpConsole con = HttpConsole
            .create(Net.getFreePort())
            .start();

        String prefix = "http://localhost:" + con.getPort();
        assertNotNull(Http.doGet(prefix + "/varz"));
        assertNotNull(Http.doGet(prefix + "/propz"));
        assertNotNull(Http.doGet(prefix + "/"));
    }

    /**
     * Make sure we throw exception for null widget
     */
    @Test (expected = NullPointerException.class)
    public void testAddWidgetWithNullServlet() throws Exception {
        HttpConsole con = HttpConsole
            .create(Net.getFreePort())
            .start()
            .addWidget(null, "/failz/*");
    }

    /**
     * Make sure we throw exception for null path
     */
    @Test (expected = NullPointerException.class)
    public void testAddWidgetWithNullPath() throws Exception {
        HttpConsole con = HttpConsole
            .create(Net.getFreePort())
            .start()
            .addWidget(new SystemPropertiesWidget(), null);
    }

    /**
     * Make sure we throw exception if user tries to register widget
     * on root path
     */
    @Test (expected = IllegalArgumentException.class)
    public void testAddWidgetWithRootPath() throws Exception {
        HttpConsole con = HttpConsole
            .create(Net.getFreePort())
            .start()
            .addWidget(new SystemPropertiesWidget(), "/");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testAddWidgetWithRootPathStar() throws Exception {
        HttpConsole con = HttpConsole
            .create(Net.getFreePort())
            .start()
            .addWidget(new SystemPropertiesWidget(), "/*");
    }

    /**
     * Make sure we throw exception if user tries to map widget to
     * path which already exists.
     */
    @Test (expected = IllegalStateException.class)
    public void testAddWidgetToExistingPath() throws Exception {
        HttpConsole con = HttpConsole
            .create(Net.getFreePort())
            .start()
            .addWidget(new SystemPropertiesWidget(), "/foo")
            .addWidget(new SystemPropertiesWidget(), "/foo");
    }
}