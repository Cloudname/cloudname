package org.cloudname.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.cloudname.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class keeps track of serviceStatus and endpoints for a coordinate.
 * It has an inner class for building an instance (Builder).
 *
 * TODO(dybdahl): Add support for claiming an existing node. This could be used for
 * recovery after network failure etc.
 *
 * @author dybdahl
 */
public class SingleExpressionResolver implements Watcher, ZkUserInterface {
 
    private Storage storage = Storage.NO_CONNECTION;

    private int lastStatusVersion = -1000;
    private StatusAndEndpoints statusAndEndpoints = null;
    
    private static final Logger log = Logger.getLogger(SingleExpressionResolver.class.getName());
    private ZooKeeper zk;
    private final String path;
    private List<CoordinateListener> coordinateListenerList =
            Collections.synchronizedList(new ArrayList<CoordinateListener>());


    /**
     * Constructor, the ZooKeeper instances is retrieved from implementing the ZkUserInterface so the object
     * is not ready to be used before the ZooKeeper instance is received.
     * @param path is the path of the status of the coordinate.
     */
    public SingleExpressionResolver(String path) {
        this.path = path;
    }


    @Override
    public void zooKeeperDown() {
        log.info("MyServerCoordinate: Got event ZooKeeper is down.");
        synchronized (this) {
            zk = null;
            storage = Storage.NO_CONNECTION;
        }
        updateCoordinateListeners(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                "Got message from parent watcher.");
    }

    
    @Override
    public void newZooKeeperInstance(ZooKeeper zk) {
        synchronized (this) {
            this.zk = zk;
        }
    }

    @Override
    public void timeEvent() {
    }

   
    private enum Storage {
        NO_CONNECTION,
        SYNCED,
    }

    /**
     * Returns the ServiceStatus. If we claimed the coordinate, this is the real-time value, otherwise
     * it is the last loaded value.
     * @return ServiceStatus.
     */
    public ServiceStatus getServiceStatus() {
        return statusAndEndpoints.getServiceStatus();
    }

    /**
     * Returns the Endpoint. If we claimed the coordinate, this is the real-time value, otherwise
     * it is the last loaded value.
     * @return Endpoint.
     */
    public Endpoint getEndpoint(String name) {
        return statusAndEndpoints.getEndpoint(name);
    }

    /**
     * Set all endpoints to the endpoints argument.
     * @param endpoints The endpoints are put in this list.
     */
    public void returnAllEndpoints(List<Endpoint> endpoints) {
        statusAndEndpoints.returnAllEndpoints(endpoints);
    }



    /**
     * Creates a string for debugging etc
     * @return serialized version of the instance data.
     */
    public String toString() {
        return statusAndEndpoints.toString();
    }

    /**
     * Registers a coordinatelistener that will receive events when there are changes to the status node.
     * Don't do any heavy lifting in the callback and don't call cloudname from the callback as this might create
     * a deadlock.
     * @param coordinateListener
     */
    public void registerCoordinateListener(CoordinateListener coordinateListener) throws CloudnameException {
        // List is synchronized.
        coordinateListenerList.add(coordinateListener);
    }

    /**
     * Handles even from ZooKeeper for this coordinate.
     * @param event
     */
    @Override public void process(WatchedEvent event) {
        log.info("Got an event from ZooKeeper " + event.toString());


        // If we lost connection, we don't attempt to register another watcher as this might be blocking forever.
        // Parent might try to reconnect.
        if (event.getType() == Event.EventType.None &&
                (event.getState() == Event.KeeperState.Disconnected
                        || event.getState() == Event.KeeperState.AuthFailed)) {
            updateCoordinateListeners(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE, event.toString());
            return;
        }
        if (event.getType() == Event.EventType.None &&
                (event.getState() == Event.KeeperState.Expired)) {
            updateCoordinateListeners(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE, event.toString());
            return;
        }

        // If node is deleted, we have no node to place a new watcher so we stop watching.
        if (event.getType() == Event.EventType.NodeDeleted) {
            updateCoordinateListeners(CoordinateListener.Event.NOT_OWNER, event.toString());
            return;
        }

        if (event.getType() == Event.EventType.NodeDataChanged) {
            updateCoordinateListeners(CoordinateListener.Event.COORDINATE_OUT_OF_SYNC, event.toString());
            return;

        }

        // Seems like we are connected, do sanity checking after we have registered a new watcher just to be safe.
        if (event.getState() == Event.KeeperState.SyncConnected) {
            log.info("Signing up for more events.");
        } else {
            log.log(Level.WARNING, "Got some events I don't know how to handle, sanity checking " +
                    "(hopefully this will not create another event):" + event.toString());
        }
        try {
            registerWatcher();
        } catch (CloudnameException e) {
            updateCoordinateListeners(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                    "Failed setting up new watcher, CloudnameException.");
            return;
        } catch (InterruptedException e) {
            updateCoordinateListeners(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                    "Failed setting up new watcher, InterruptedException.");
            return;
        }
        log.info("Checking state of coordinate and sending event: " + path);

    }

    /**
     * Safe way to get a ZooKeeper instance.
     * @throws CloudnameException if connection is not up.
     */
    private ZooKeeper getZooKeeper()  {
        synchronized (this) {
            return zk;
        }
    }

    /**
     * Sends an event too all coordinate listeners. Note that the event is sent from this thread so if the
     * callback code does the wrong calls, deadlocks might occur.
     * @param event
     * @param message
     */
    private void updateCoordinateListeners(CoordinateListener.Event event, String message) {
        log.info("Event " + event.name() + " " + message);
        for (CoordinateListener listener : coordinateListenerList) {
            listener.onCoordinateEvent(event, message);
        }

        synchronized (this) {
            if (event == CoordinateListener.Event.COORDINATE_OK) {
                storage = Storage.SYNCED;
                return;
            }
            storage = Storage.NO_CONNECTION;
        }
    }

    /**
     * Loads the coordinate from ZooKeeper.
     * @return this.
     */

    public SingleExpressionResolver load(Watcher watcher) throws CloudnameException {
        Stat stat = new Stat();
        try {
            byte[] data;
            if (watcher == null) {
                data = getZooKeeper().getData(path, false, stat);
            } else {
                data = getZooKeeper().getData(path, watcher, stat);
            }

            statusAndEndpoints = new StatusAndEndpoints.Builder().deserialize(data).build();

        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (UnsupportedEncodingException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (IOException e) {
            throw new CloudnameException(e);
        }
        return this;
    }

    private void registerWatcher() throws CloudnameException, InterruptedException {
        try {
            getZooKeeper().exists(path, this);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        }
    }
}