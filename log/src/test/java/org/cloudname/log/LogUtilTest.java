package org.cloudname.log;

import org.cloudname.log.pb.Timber;
import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for the LogUtil class.
 *
 * @author borud
 */
public class LogUtilTest {

    @Test
    public void testTextEvent() throws Exception
    {
        Timber.LogEvent event = LogUtil.textEvent(20,
                                                  "some.service",
                                                  "the.source",
                                                  "the message");
        assertEquals(20, event.getLevel());
        assertEquals("some.service", event.getServiceName());
        assertEquals("the.source", event.getSource());
        assertEquals("the message", event.getPayload(0).getPayload().toStringUtf8());

        assertTrue((System.currentTimeMillis() - event.getTimestamp()) < 10);
    }

    /**
     * Test that we manage to get some hostname from the getHostName()
     * method.  This may fail on some machines.  We do leave this test
     * in here on purpose because it might help us to identify how to
     * manage on machines that exhibit this behavior.
     */
    @Test
    public void testHostName() throws Exception
    {
        assertNotNull(LogUtil.getHostName());
    }
}
