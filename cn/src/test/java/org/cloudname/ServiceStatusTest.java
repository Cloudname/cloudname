package org.cloudname;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for ServiceStatus.
 *
 * @author borud
 */
public class ServiceStatusTest {
    @Test
    public void testSimple() throws Exception {
        ServiceStatus status = new ServiceStatus(ServiceState.STARTING,
                                                 "Loading hamster into wheel");
        String json = status.toJson();
        assertNotNull(json);

        ServiceStatus status2 = ServiceStatus.fromJson(json);

        assertEquals(status.getMessage(), status2.getMessage());
        assertSame(status.getState(), status2.getState());
    }
}