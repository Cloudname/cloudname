package org.cloudname.zk;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.cloudname.Coordinate;
import org.cloudname.CoordinateException;
import org.cloudname.CoordinateListener.Event;
import org.cloudname.ServiceHandle;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ZKServiceHandleTest {

    private final EmbeddedZooKeeperWithClient embeddedZookeeper =
            new EmbeddedZooKeeperWithClient();
    private ZkCloudname cn;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Before
    public void startEmbeddedZookeeper() throws Exception {
        embeddedZookeeper.setup(temp);
    }

    @Test
    public void testServiceHandleConvenienceMethod() throws Exception {
        cn =
                new ZkCloudname.Builder()
                        .setConnectString("localhost:" + embeddedZookeeper.getZkPort()).build()
                        .connect();

        final Coordinate coordinate1 = Coordinate.parse("1.service.user.cell");

        try {
            cn.createCoordinate(coordinate1);
        } catch (final CoordinateException e) {
            fail(e.toString());
        }

        final ServiceHandle serviceHandle1 = cn.claim(coordinate1);
        final boolean waitForCoordinateOk =
                serviceHandle1.waitForCoordinateOkSeconds(1);
        assertTrue(waitForCoordinateOk);
    }

    @Test
    public void testServiceHandle() throws Exception {
        cn =
                new ZkCloudname.Builder()
                        .setConnectString("localhost:" + embeddedZookeeper.getZkPort()).build()
                        .connect();

        final Coordinate coordinate1 = Coordinate.parse("1.service.user.cell");

        try {
            cn.createCoordinate(coordinate1);
        } catch (final CoordinateException e) {
            fail(e.toString());
        }

        final ServiceHandle serviceHandle1 = cn.claim(coordinate1);
        final boolean waitForCoordinateOk =
                serviceHandle1.waitForCoordinateEventSeconds(1, Event.COORDINATE_OK);
        final boolean waitForNoConnection =
                serviceHandle1.waitForCoordinateEventSeconds(1, Event.NO_CONNECTION_TO_STORAGE);

        assertTrue(waitForCoordinateOk);
        assertFalse(waitForNoConnection);
    }

    public void tearDown() throws Exception {
        embeddedZookeeper.close();
    }
}
