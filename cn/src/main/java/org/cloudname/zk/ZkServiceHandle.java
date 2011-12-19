package org.cloudname.zk;

import org.cloudname.*;

import java.util.ArrayList;
import java.util.logging.Logger;

import java.util.List;

/**
 * A service handle implementation.
 *
 * @author borud
 */
public class ZkServiceHandle implements ServiceHandle {
    private Coordinate coordinate;
    //private ZooKeeper zk;
    private ZkStatusEndpoint statusEndpoint;
    private static final Logger log = Logger.getLogger(ZkServiceHandle.class.getName());

    /**
     * Create a ZkServiceHandle for a given coordinate.
     *
     * This constructor is slightly evil since it does IO, but if
     * any of the IO operations fail the object is irrelevant
     * anyway.
     *
     * TODO(borud): expand error handling.
     *
     * @param coordinate the coordinate for this service handle.
     */
    public ZkServiceHandle(Coordinate coordinate, ZkStatusEndpoint statusEndpoint) {
        this.coordinate = coordinate;


        this.statusEndpoint = statusEndpoint;
        assert(statusEndpoint != null);
    }

    @Override
    public void setStatus(ServiceStatus status) {
        statusEndpoint.updateStatus(status);
    }


    @Override
    public void putEndpoints(List<Endpoint> endpoints) {
        statusEndpoint.putEndpoints(endpoints);
    }

    @Override
    public void putEndpoint(Endpoint endpoint) {
        List<Endpoint> endpoints = new ArrayList<Endpoint>();
        endpoints.add(endpoint);
        putEndpoints(endpoints);
    }

    @Override
    public void removeEndpoints(List<String> names) {
        statusEndpoint.removeEndpoints(names);
    }

    @Override
    public void removeEndpoint(String name) {
        List<String> names = new ArrayList<String>();
        names.add(name);
        removeEndpoints(names);
    }

    @Override
    public void registerConfigListener(ConfigListener listener) {

    }

    @Override
    public void close() {
        statusEndpoint.deleteClaimed();
    }

    @Override
    public String toString() {
        return "TODO";
    }

    /**
     * Only used by the ZkCloudname class to invalidate
     * ServiceHandles. (Package private).
     */
    public boolean isOpen() {
        return false;
    }
}