package org.cloudname.zk;

import org.apache.zookeeper.ZooKeeper;
import org.cloudname.*;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import java.util.List;

/**
 * A service handle implementation.
 *
 * @author borud
 */
public class ZkServiceHandle implements ServiceHandle, ZkUserInterface {
    private final Coordinate coordinate;
    private ZkLocalStatusAndEndpoints statusAndEndpoints;
    private static final Logger log = Logger.getLogger(ZkServiceHandle.class.getName());
    private ZooKeeper zk = null;

    /**
     * Create a ZkServiceHandle for a given coordinate.
     * TODO(borud, dybdahl): Implement config listener.
     *
     * @param coordinate the coordinate for this service handle.
     */
    public ZkServiceHandle(Coordinate coordinate, ZkLocalStatusAndEndpoints statusAndEndpoints) {
        this.coordinate = coordinate;
        this.statusAndEndpoints = statusAndEndpoints;
    }

    @Override
    public boolean waitForConnectionToStorageMillis(long millis) throws CloudnameException {
        final CountDownLatch latch = new CountDownLatch(1);
        registerCoordinateListener(new CoordinateListener() {
            
            @Override
            public boolean onConfigEvent(Event event, String message) {
                //System.out.println("%%%%%%%%%%%%%%%%%%%" + event.name() + " " + message);
                if (event == Event.COORDINATE_OK) {
                    System.out.println("(((((((((((())))))) LATCHING");
                    latch.countDown();
                    return false;
                }
                return true;
            }
        });
        try {
            System.out.println("WAITIN LATCH!!");
            boolean  r = latch.await(millis, TimeUnit.MILLISECONDS);
            System.out.println("WAITIN LATCH   DDDOOBNE!!");

            return r;
        } catch (InterruptedException e) {
            return false;
        }
    }

    @Override
    public StorageOperation setStatus(ServiceStatus status) throws CloudnameException, CoordinateMissingException {
        statusAndEndpoints.updateStatus(status);
        return createStorageOperation();
    }

    private StorageOperation createStorageOperation() {
        final ZkStorageOperation op = new ZkStorageOperation();

        registerCoordinateListener(new CoordinateListener() {

            @Override
            public boolean onConfigEvent(Event event, String message) {
                if (event == Event.COORDINATE_OK) {
                    op.getSystemCallback().success();
                    return false;
                }
                return true;
            }
        });
        return op;
    }

    @Override
    public StorageOperation putEndpoints(List<Endpoint> endpoints)
            throws EndpointException, CloudnameException, CoordinateMissingException {
        statusAndEndpoints.putEndpoints(endpoints);
        return createStorageOperation();
    }

    @Override
    public StorageOperation putEndpoint(Endpoint endpoint)
            throws EndpointException, CloudnameException, CoordinateMissingException {
        List<Endpoint> endpoints = new ArrayList<Endpoint>();
        endpoints.add(endpoint);
        putEndpoints(endpoints);
        return createStorageOperation();
    }

    @Override
    public StorageOperation removeEndpoints(List<String> names)
            throws EndpointException, CloudnameException, CoordinateMissingException {
        statusAndEndpoints.removeEndpoints(names);
        return createStorageOperation();
    }

    @Override
    public StorageOperation removeEndpoint(String name) throws EndpointException, CloudnameException, CoordinateMissingException {
        List<String> names = new ArrayList<String>();
        names.add(name);
        removeEndpoints(names);
        return createStorageOperation();
    }

    @Override
    public void registerConfigListener(ConfigListener listener) {

    }

    @Override
    public void registerCoordinateListener(CoordinateListener listener)  {
        statusAndEndpoints.registerCoordinateListener(listener);
    }

    @Override
    public void close() throws CloudnameException {

        statusAndEndpoints.releaseClaim();

        statusAndEndpoints = null;
    }

    @Override
    public String toString() {
        return "StatusEndpoint instance: "+ statusAndEndpoints.toString();
    }

    @Override
    public void zooKeeperDown() {
        synchronized (this) {
            zk = null;
        }
    }

    @Override
    public void newZooKeeperInstance(ZooKeeper zk) {
        synchronized (this)  {
            this.zooKeeperDown();
        }
    }

    @Override
    public void wakeUp() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}