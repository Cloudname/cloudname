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
    public void setStatus(ServiceStatus status) throws CloudnameException, CoordinateMissingException {
        statusAndEndpoints.updateStatus(status);
    }

    @Override
    public void putEndpoints(List<Endpoint> endpoints)
            throws EndpointException, CloudnameException, CoordinateMissingException {
        statusAndEndpoints.putEndpoints(endpoints);
    }

    @Override
    public void putEndpoint(Endpoint endpoint)
            throws EndpointException, CloudnameException, CoordinateMissingException {
        List<Endpoint> endpoints = new ArrayList<Endpoint>();
        endpoints.add(endpoint);
        putEndpoints(endpoints);
    }

    @Override
    public void removeEndpoints(List<String> names)
            throws EndpointException, CloudnameException, CoordinateMissingException {
        statusAndEndpoints.removeEndpoints(names);
    }

    @Override
    public void removeEndpoint(String name) throws EndpointException, CloudnameException, CoordinateMissingException {
        List<String> names = new ArrayList<String>();
        names.add(name);
        removeEndpoints(names);
    }

    @Override
    public void registerConfigListener(ConfigListener listener) {

    }

    @Override
    public void registerCoordinateListener(CoordinateListener listener) throws CloudnameException {
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