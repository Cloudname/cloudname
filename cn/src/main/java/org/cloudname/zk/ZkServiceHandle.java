package org.cloudname.zk;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.cloudname.CloudnameException;
import org.cloudname.CloudnameLock;
import org.cloudname.ConfigListener;
import org.cloudname.Coordinate;
import org.cloudname.CoordinateListener;
import org.cloudname.CoordinateListener.Event;
import org.cloudname.CoordinateMissingException;
import org.cloudname.Endpoint;
import org.cloudname.ServiceHandle;
import org.cloudname.ServiceStatus;

/**
 * A service handle implementation. It does not have a lot of logic, it wraps ClaimedCoordinate, and
 * handles some config logic.
 *
 * @author borud
 */
public class ZkServiceHandle implements ServiceHandle {
    private final ClaimedCoordinate claimedCoordinate;
    private static final Logger LOG = Logger.getLogger(ZkServiceHandle.class.getName());

    private final ZkObjectHandler.Client zkClient;

    private final Coordinate coordinate;
    
    /**
     * Create a ZkServiceHandle for a given coordinate.
     *
     * @param claimedCoordinate the claimed coordinate for this service handle.
     */
    public ZkServiceHandle(
            ClaimedCoordinate claimedCoordinate, Coordinate coordinate,
            ZkObjectHandler.Client zkClient) {
        this.claimedCoordinate = claimedCoordinate;
        this.coordinate = coordinate;
        this.zkClient = zkClient;
    }

    @Override
    public boolean waitForCoordinateOkSeconds(final int seconds) throws InterruptedException {
        return waitForCoordinateEventSeconds(seconds, Event.COORDINATE_OK);
    }

    @Override
    public boolean waitForCoordinateEventSeconds(final int seconds, final Event expectedEvent)
            throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CoordinateListener listner = new CoordinateListener() {
            @Override
            public void onCoordinateEvent(final Event event, final String message) {
                if (event == expectedEvent) {
                    latch.countDown();
                }
            }
        };
        registerCoordinateListener(listner);
        final boolean result = latch.await(seconds, TimeUnit.SECONDS);
        claimedCoordinate.deregisterCoordinateListener(listner);
        return result;
    }

    @Override
    public void setStatus(ServiceStatus status)
            throws CoordinateMissingException, CloudnameException {
        claimedCoordinate.updateStatus(status);
    }

    @Override
    public void putEndpoints(List<Endpoint> endpoints)
            throws CoordinateMissingException, CloudnameException {
        claimedCoordinate.putEndpoints(endpoints);
    }

    @Override
    public void putEndpoint(Endpoint endpoint)
            throws CoordinateMissingException, CloudnameException {
        List<Endpoint> endpoints = new ArrayList<Endpoint>();
        endpoints.add(endpoint);
        putEndpoints(endpoints);
    }

    @Override
    public void removeEndpoints(List<String> names)
            throws CoordinateMissingException, CloudnameException {
        claimedCoordinate.removeEndpoints(names);
    }

    @Override
    public void removeEndpoint(String name)
            throws CoordinateMissingException, CloudnameException {
        List<String> names = new ArrayList<String>();
        names.add(name);
        removeEndpoints(names);
    }

    @Override
    public void registerConfigListener(ConfigListener listener) {
        TrackedConfig trackedConfig = new TrackedConfig(
                ZkCoordinatePath.getConfigPath(coordinate, null), listener, zkClient);
        claimedCoordinate.registerTrackedConfig(trackedConfig);
        trackedConfig.start();
    }

    @Override
    public void registerCoordinateListener(CoordinateListener listener)  {
        claimedCoordinate.registerCoordinateListener(listener);
    }

    @Override
    public void close() throws CloudnameException {
        claimedCoordinate.releaseClaim();
    }

    @Override
    public CloudnameLock getCloudnameLock(CloudnameLock.Scope scope, String lockName) {
        return claimedCoordinate.getCloudnameLock(scope, lockName);
    }

    @Override
    public String toString() {
        return "Claimed coordinate instance: "+ claimedCoordinate.toString();
    }
}
