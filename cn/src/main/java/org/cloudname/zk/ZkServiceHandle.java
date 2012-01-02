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
    private ZkStatusAndEndpoints statusAndEndpoints;
    private static final Logger log = Logger.getLogger(ZkServiceHandle.class.getName());

    /**
     * Create a ZkServiceHandle for a given coordinate.
     * TODO(borud, dybdahl): Implement config listener.
     *
     * @param coordinate the coordinate for this service handle.
     */
    public ZkServiceHandle(Coordinate coordinate, ZkStatusAndEndpoints statusAndEndpoints) {
        this.coordinate = coordinate;
        this.statusAndEndpoints = statusAndEndpoints;
    }

    @Override
    public void setStatus(ServiceStatus status) {
        statusAndEndpoints.updateStatus(status);
    }

    @Override
    public void putEndpoints(List<Endpoint> endpoints) {
        statusAndEndpoints.putEndpoints(endpoints);
    }

    @Override
    public void putEndpoint(Endpoint endpoint) {
        List<Endpoint> endpoints = new ArrayList<Endpoint>();
        endpoints.add(endpoint);
        putEndpoints(endpoints);
    }

    @Override
    public void removeEndpoints(List<String> names) {
        statusAndEndpoints.removeEndpoints(names);
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
        statusAndEndpoints.releaseClaim();
        statusAndEndpoints = null;
    }

    @Override
    public String toString() {
        return "StatusEndpoint instance: "+ statusAndEndpoints.toString();
    }
}