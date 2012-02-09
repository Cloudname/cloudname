package org.cloudname.zk;

import org.apache.zookeeper.ZooKeeper;
import org.cloudname.*;

import java.util.ArrayList;
import java.util.logging.Logger;

import java.util.List;

/**
 * A service handle implementation.
 *
 * @author borud
 */
public class ZkServiceHandle implements ServiceHandle, ZkUserInterface {
    private final Coordinate coordinate;
    private MyServerCoordinate statusAndEndpoints;
    private static final Logger log = Logger.getLogger(ZkServiceHandle.class.getName());
    private ZooKeeper zk = null;

    /**
     * Create a ZkServiceHandle for a given coordinate.
     * TODO(borud, dybdahl): Implement config listener.
     *
     * @param coordinate the coordinate for this service handle.
     */
    public ZkServiceHandle(Coordinate coordinate, MyServerCoordinate statusAndEndpoints) {
        this.coordinate = coordinate;
        this.statusAndEndpoints = statusAndEndpoints;
    }



    @Override
    public StorageFuture setStatus(ServiceStatus status) {
        try {
            statusAndEndpoints.updateStatus(status);
        } catch (CloudnameException e) {
           return new ZkStorageFuture("CloudnameException:" + e.getMessage());
        } catch (CoordinateMissingException e) {
            return new ZkStorageFuture("CoordinateMissingException:" + e.getMessage());
        }
        return createStorageOperation();
    }

    private StorageFuture createStorageOperation() {
        final ZkStorageFuture op = new ZkStorageFuture();

        registerCoordinateListener(new CoordinateListener() {

            @Override
            public boolean onCoordinateEvent(Event event, String message) {
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
    public StorageFuture putEndpoints(List<Endpoint> endpoints) {
        try {
            statusAndEndpoints.putEndpoints(endpoints);
        } catch (EndpointException e) {
            return new ZkStorageFuture("EndpointException: " + e.getMessage());
        } catch (CloudnameException e) {
            return new ZkStorageFuture("CloudnameException: " + e.getMessage());
        } catch (CoordinateMissingException e) {
            return new ZkStorageFuture("CoordinateMissingException: " + e.getMessage());
        }
        return createStorageOperation();
    }

    @Override
    public StorageFuture putEndpoint(Endpoint endpoint) {
        List<Endpoint> endpoints = new ArrayList<Endpoint>();
        endpoints.add(endpoint);
        putEndpoints(endpoints);
        return createStorageOperation();
    }

    @Override
    public StorageFuture removeEndpoints(List<String> names) {
        try {
            statusAndEndpoints.removeEndpoints(names);
        } catch (EndpointException e) {
            return new ZkStorageFuture("EndpointException: " + e.getMessage());
        } catch (CloudnameException e) {
            return new ZkStorageFuture("CloudnameException: " + e.getMessage());
        } catch (CoordinateMissingException e) {
            return new ZkStorageFuture("CoordinateMissingException: " + e.getMessage());
        }
        return createStorageOperation();
    }

    @Override
    public StorageFuture removeEndpoint(String name) {
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
    public void timeEvent() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}