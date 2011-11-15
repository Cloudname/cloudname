package org.cloudname.log.recordstore;

import java.io.ByteArrayOutputStream;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for CountingOutputStream.
 *
 * @author borud
 */
public class CountingOutputStreamTest {
    @Test
    public void testCounting() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CountingOutputStream c = new CountingOutputStream(baos);

        // Make sure everything is nice and tidy initially
        assertEquals(0, c.getBytesWritten());
        assertEquals(0, c.getLastBytesWritten());

        // Make sure adding single bytes works
        c.write(1);
        assertEquals(1, c.getBytesWritten());
        assertEquals(1, c.getLastBytesWritten());

        c.write(1);
        assertEquals(2, c.getBytesWritten());
        assertEquals(1, c.getLastBytesWritten());

        // Make sure empty array works
        c.write(new byte[] {});
        assertEquals(2, c.getBytesWritten());
        assertEquals(0, c.getLastBytesWritten());

        // Make sure adding arrays works
        c.write(new byte[] {1,2,3,4});
        assertEquals(6, c.getBytesWritten());
        assertEquals(4, c.getLastBytesWritten());

        // Make sure adding arrays with offsets works
        c.write(new byte[] {1,2,3,4}, 0, 3);
        assertEquals(9, c.getBytesWritten());
        assertEquals(3, c.getLastBytesWritten());

        c.write(new byte[] {1,2,3,4}, 2, 2);
        assertEquals(11, c.getBytesWritten());
        assertEquals(2, c.getLastBytesWritten());
    }
}