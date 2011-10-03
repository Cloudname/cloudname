package org.cloudname.testtools;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for Http class.
 *
 * @author borud
 */
public class HttpTest {

    /**
     * This is slightly evil and requires the machine to be connected
     * to the Internet to work, but hey...
     */
    @Test
    public void testSimple() throws Exception {
        String content = Http.doGet("http://www.google.com/");
        assertNotNull(content);
        assertTrue(content.length() > 0);
    }
}