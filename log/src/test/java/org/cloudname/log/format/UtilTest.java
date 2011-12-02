package org.cloudname.log.format;

import java.util.logging.Level;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for Util class.
 *
 * @author borud
 */
public class UtilTest {
    // Make sure we map the log levels in Java correctly
    @Test
    public void testLevelNameForValue() throws Exception {
        for (Level level : new Level[] {
                Level.CONFIG,
                Level.FINEST,
                Level.FINER,
                Level.FINE,
                Level.INFO,
                Level.WARNING,
                Level.SEVERE,}) {
            assertEquals(level.getName(), Util.logLevelNameForValue(level.intValue()));
        }

        // Check that anything more severe that WARNING will produce
        // expected result.
        assertEquals("NUCLEAR", Util.logLevelNameForValue(Level.SEVERE.intValue() + 1));
    }

    @Test
    public void testFormatTimeSecondsSinceEpoch() throws Exception {
        {
            StringBuilder buff = new StringBuilder();
            Util.formatTimeSecondsSinceEpoch(10001L, buff);
            assertEquals("10.001", buff.toString());
        }
        {
            // This is a degenerate case
            StringBuilder buff = new StringBuilder();
            Util.formatTimeSecondsSinceEpoch(10L, buff);
            assertEquals("10", buff.toString());
        }
    }

    @Test
    public void testFormatTimeISO() throws Exception {
        StringBuilder buff = new StringBuilder();
        Util.formatTimeISO(1322604315123L, buff);
        assertEquals("2011-11-29T22:05:15.123", buff.toString());
    }

    @Test
    public void testEscape() throws Exception {
        assertEquals("\\n\\t\\n\\t", Util.escape("\n\t\n\t"));
        assertEquals("\\\\\\\\", Util.escape("\\\\"));
    }
}
