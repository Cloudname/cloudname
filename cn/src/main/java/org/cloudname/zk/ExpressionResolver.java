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
 * It has an inner class for building an instance (Dynamic).
 *
 *
 * @author dybdahl
 */
public class ExpressionResolver implements Watcher, ZkUserInterface {
 
    private Storage storage = Storage.NO_CONNECTION;

    private int lastStatusVersion = -1000;
    private CoordinateData.Snapshot coordinateData = null;
    
    private static final Logger log = Logger.getLogger(ExpressionResolver.class.getName());
    private ZooKeeper zk;
    private final String path;
    private List<CoordinateListener> coordinateListenerList =
            Collections.synchronizedList(new ArrayList<CoordinateListener>());


    /**
     * Constructor, the ZooKeeper instances is retrieved from implementing the ZkUserInterface so the object
     * is not ready to be used before the ZooKeeper instance is received.
     * @param path is the path of the status of the coordinate.
     */
    public ExpressionResolver(String path) {
        this.path = path;
    }


    @Override
    public void zooKeeperDown() {
        log.fine("ClaimedCoordinate: Got event ZooKeeper is down.");
        synchronized (this) {
            zk = null;
            storage = Storage.NO_CONNECTION;
        }
        updateCoordinateListeners(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                "Got message from parent watcher.");
    }

    
    @Override
    public void newZooKeeperInstance(ZooKeeper zk) {
        log.fine("ClaimedCoordinate: Got new ZooKeeper.");
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
        return coordinateData.getServiceStatus();
    }

    /**
     * Returns the Endpoint. If we claimed the coordinate, this is the real-time value, otherwise
     * it is the last loaded value.
     * @return Endpoint.
     */
    public Endpoint getEndpoint(String name) {
        return coordinateData.getEndpoint(name);
    }

    /**
     * Set all endpoints to the endpoints argument.
     * @param endpoints The endpoints are put in this list.
     */
    public void returnAllEndpoints(List<Endpoint> endpoints) {
        coordinateData.appendAllEndpoints(endpoints);
    }



    /**
     * Creates a string for debugging etc
     * @return serialized version of the instance data.
     */
    public String toString() {
        return coordinateData.toString();
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
        log.fine("Got an event from ZooKeeper " + event.toString() + " path: " + path);

        switch (event.getType()) {
            case None:
                switch (event.getState()) {
                    case Disconnected:
                    case AuthFailed:
                    case Expired:
                    default:
                        // If we lost connection, we don't attempt to register another watcher as this might
                        // be blocking forever. Parent might try to reconnect.
                        updateCoordinateListeners(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE, event.toString());
                        return;
                    case SyncConnected:
                }
                break;
            case NodeDeleted:
                // If node is deleted, we have no node to place a new watcher so we stop watching.
                updateCoordinateListeners(CoordinateListener.Event.NOT_OWNER, event.toString());
                return;
            case NodeDataChanged:
                updateCoordinateListeners(CoordinateListener.Event.COORDINATE_OUT_OF_SYNC, event.toString());
                return;
            case NodeChildrenChanged:
            case NodeCreated:
                log.info("Unexpected event, ignoring, try to register new listener to get next event, path: " + path);
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
        }
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

    public ExpressionResolver load(Watcher watcher) throws CloudnameException {
        Stat stat = new Stat();
        try {
            byte[] data;
            if (watcher == null) {
                data = getZooKeeper().getData(path, false, stat);
            } else {
                data = getZooKeeper().getData(path, watcher, stat);
            }

            coordinateData = new CoordinateData().deserialize(data).snapshot();

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