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
public class MyServerCoordinate implements Watcher, ZkUserInterface {

    private Storage storage = Storage.OUT_OF_SYNC;
    private boolean started = false;

    private int lastStatusVersion = -1;
    private static final Logger log = Logger.getLogger(MyServerCoordinate.class.getName());
    private ZooKeeper zk;
    private final String path;

    private StatusAndEndpoints.Builder statusAndEndpointsBuilder = new StatusAndEndpoints.Builder();
    private List<CoordinateListener> coordinateListenerList =
            Collections.synchronizedList(new ArrayList<CoordinateListener>());


    /**
     * Constructor, the ZooKeeper instances is retrieved from implementing the ZkUserInterface so the object
     * is not ready to be used before the ZooKeeper instance is received.
     * @param path is the path of the status of the coordinate.
     */
    public MyServerCoordinate(String path) {
        this.path = path;
    }

    @Override
    public void zooKeeperDown() {
        log.info("MyServerCoordinate: Got event ZooKeeper is down.");
        synchronized (this) {
            zk = null;
            storage = Storage.OUT_OF_SYNC;
        }
        sendEventToCoordinateListener(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                "Got message from parent watcher.");
    }

    class ClaimCallback implements AsyncCallback.StringCallback {

        @Override
        public void processResult(int rawReturnCode, String s, Object parent, String s1) {

            KeeperException.Code returnCode =  KeeperException.Code.get(rawReturnCode);
            MyServerCoordinate statusAndEndpoints =  (MyServerCoordinate) parent;
           
            switch (returnCode) {

                case OK:
                    synchronized (statusAndEndpoints) {
                        try {
                            lastStatusVersion = -1;
                            statusAndEndpoints.writeStatusEndpoint();
                            storage = Storage.SYNCED;

                        } catch (CoordinateMissingException e) {
                            log.info("Problems writing config, coordinate missing");
                            statusAndEndpoints.sendEventToCoordinateListener(
                                    CoordinateListener.Event.NOT_OWNER,
                                    "Can not write config: " + returnCode.name() + " " + s);
                            break;
                        } catch (CloudnameException e) {
                            log.info("Problems writing config." + e.getMessage());
                            statusAndEndpoints.sendEventToCoordinateListener(
                                    CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                                    "Can not write config after claim: " + returnCode.name() + " " + s);
                            break;
                        }
                    }

                    try {
                        registerWatcher();
                    } catch (CloudnameException e) {
                        log.info("Cloudname exception. Going to state out of sync: " + e.getMessage());
                        storage = Storage.OUT_OF_SYNC;
                        break;

                    } catch (InterruptedException e) {
                        log.info("Interrupted while setting up new watcher. Going to state out of sync.");
                        storage = Storage.OUT_OF_SYNC;
                        break;
                    }
                    statusAndEndpoints.sendEventToCoordinateListener(CoordinateListener.Event.COORDINATE_OK, "claimed");
                    break;
                case NODEEXISTS:
                    synchronized (statusAndEndpoints) {
                        // If everything is fine, this is not a true negative, so ignore it.
                        if (storage == Storage.SYNCED && started) {
                            break;
                        }
                    }
                    statusAndEndpoints.sendEventToCoordinateListener(
                            CoordinateListener.Event.NOT_OWNER, "Node already exists.");
                    break;
                case NONODE:
                    statusAndEndpoints.sendEventToCoordinateListener(
                            CoordinateListener.Event.NOT_OWNER,
                            "No node on claiming coordinate: " + returnCode.name() + " " + s);
                    break;
                
                default:
                     statusAndEndpoints.sendEventToCoordinateListener(
                             CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                             "Could not reclaim coordinate. Return code: " + returnCode.name() + " " + s);
            }
        }
    }

    @Override
    public void newZooKeeperInstance(ZooKeeper zk) {
        log.info("MyServerCoordinate: Got new ZeeKeeper, expect session to be down so starting potential cleanup."
                + zk.getSessionId());
        synchronized (this) {
            this.zk = zk;
            // We always start by assuming it is unclaimed.
            storage = Storage.OUT_OF_SYNC;

            if (! started) {
                return;
            }
            log.info("MyServerCoordinate: Reclaiming coordinate.");
            claim(zk);
        }
    }

    @Override
    public void timeEvent() {
        
        synchronized (this) {
            if ( storage == Storage.SYNCED || zk == null || ! started) {
                return;
            }
            System.out.println("Monitor thread sees problems, trying to reclaim.");
            claim(zk);
        }

    }


    /**
     * Updates the ServiceStatus and persists it. Only allowed if we claimed the coordinate.
     * @param status The new value for serviceStatus.
     */
    public void updateStatus(ServiceStatus status) throws CloudnameException, CoordinateMissingException {
        statusAndEndpointsBuilder.updateStatus(status);
        writeStatusEndpoint();
    }

    /**
     * Adds new endpoints and persist them. Requires that this instance owns the claim to the coordinate.
     * @param newEndpoints endpoints to be added.
     */
    public void putEndpoints(List<Endpoint> newEndpoints)
            throws EndpointException, CloudnameException, CoordinateMissingException {
        statusAndEndpointsBuilder.putEndpoints(newEndpoints);
        writeStatusEndpoint();
    }

    /**
     * Remove endpoints and persist it. Requires that this instance owns the claim to the coordinate.
     * @param names names of endpoints to be removed.
     */
    public void removeEndpoints(List<String> names)
            throws EndpointException, CloudnameException, CoordinateMissingException {
        statusAndEndpointsBuilder.removeEndpoints(names);
        writeStatusEndpoint();
    }

    /**
     * Release the claim of the coordinate. It means that nobody owns the coordinate anymore.
     * Requires that that this instance owns the claim to the coordinate.
     */
    public void releaseClaim() throws CloudnameException {
        synchronized (this) {

            try {
                getZooKeeper().delete(path, lastStatusVersion);
            } catch (InterruptedException e) {
                throw new CloudnameException(e);
            } catch (KeeperException e) {
                throw new CloudnameException(e);
            }
            statusAndEndpointsBuilder = null;
            lastStatusVersion = -1;
        }
    }

    /**
     * Creates a string for debugging etc
     * @return serialized version of the instance data.
     */
    public synchronized String toString() {
       return statusAndEndpointsBuilder.build().toString();
    }

    /**
     * Registers a coordinatelistener that will receive events when there are changes to the status node.
     * Don't do any heavy lifting in the callback and don't call cloudname from the callback as this might create
     * a deadlock.
     * @param coordinateListener
     */
    public void registerCoordinateListener(CoordinateListener coordinateListener)  {

        String message = "New listener added, resending current state.";
        synchronized (this) {
            coordinateListenerList.add(coordinateListener);
            if (storage == Storage.SYNCED) {
                coordinateListener.onCoordinateEvent(CoordinateListener.Event.COORDINATE_OK, message);
            } else {
                sendEventToCoordinateListener(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE, "Not ok " +
                        storage.toString());
            }
        }
    }

    /**
     * Claims a coordinate.
     * @return this.
     */
    public MyServerCoordinate start()  {
        synchronized (this) {
            started = true;
        }
        timeEvent();
        return this;
    }


    /**
     * Handles event from ZooKeeper for this coordinate.
     * @param event
     */
    @Override public void process(WatchedEvent event) {
        log.info("Got an event from ZooKeeper " + event.toString());


        // If we lost connection, we don't attempt to register another watcher as this might be blocking forever.
        // Parent might try to reconnect.
        if (event.getType() == Event.EventType.None &&
                (event.getState() == Event.KeeperState.Disconnected
                        || event.getState() == Event.KeeperState.AuthFailed)) {

            sendEventToCoordinateListener(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE, event.toString());
            return;
        }
        if (event.getType() == Event.EventType.None &&
                (event.getState() == Event.KeeperState.Expired)) {

            sendEventToCoordinateListener(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE, event.toString());
            return;
        }

        // If node is deleted, we have no node to place a new watcher so we stop watching.
        if (event.getType() == Event.EventType.NodeDeleted) {

            sendEventToCoordinateListener(CoordinateListener.Event.NOT_OWNER, event.toString());
            return;
        }
        if (event.getState() == Event.KeeperState.SyncConnected &&  event.getType() == Event.EventType.NodeDataChanged) {
            try {
                registerWatcher();

            } catch (CloudnameException e) {
                sendEventToCoordinateListener(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                        "Failed setting up new watcher, CloudnameException.");
                return;
            } catch (InterruptedException e) {
                sendEventToCoordinateListener(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                        "Failed setting up new watcher, InterruptedException.");
                return;
            }
            return;
        }
        if (event.getType() == Event.EventType.NodeDataChanged) {
            sendEventToCoordinateListener(CoordinateListener.Event.COORDINATE_OUT_OF_SYNC, event.toString());
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
            sendEventToCoordinateListener(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                    "Failed setting up new watcher, CloudnameException.");
            return;
        } catch (InterruptedException e) {
            sendEventToCoordinateListener(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                    "Failed setting up new watcher, InterruptedException.");
            return;
        }
        log.info("Checking state of coordinate and sending event: " + path);

    }

    private void claim(ZooKeeper zkArg) {
        try {
            zkArg.create(
                    path, statusAndEndpointsBuilder.build().serialize().getBytes(Util.CHARSET_NAME),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new ClaimCallback(), this);
        } catch (CloudnameException e) {
            log.info("Could not claim with the new ZooKeeper instance: " + e.getMessage());
        } catch (IOException e) {
            log.info("Got IO exception on claim with new ZooKeeper instance " + e.getMessage());
        }
    }

    private enum Storage {
        OUT_OF_SYNC,
        SYNCED,
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
    private void sendEventToCoordinateListener(CoordinateListener.Event event, String message) {
        synchronized (this) {
            storage = Storage.OUT_OF_SYNC;
            log.info("Event " + event.name() + " " + message);
            for (CoordinateListener listener : coordinateListenerList) {
                listener.onCoordinateEvent(event, message);
            }
        }
    }


    private void registerWatcher() throws CloudnameException, InterruptedException {
        log.info("Register watcher for ZooKeeper..");
        try {
            getZooKeeper().exists(path, this);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        }
    }


    /**
     * Creates the serialized value of the object and stores this in ZooKeeper under the path.
     * It updates the lastStatusVersion.
     */
    private void writeStatusEndpoint() throws CoordinateMissingException, CloudnameException {
        synchronized (this) {

            if (! started) {
                throw new IllegalStateException("Not started yet: " + storage.name());
            }
            try {

                Stat stat = getZooKeeper().setData(path,
                        statusAndEndpointsBuilder.build().serialize().getBytes(Util.CHARSET_NAME),
                        lastStatusVersion);
                lastStatusVersion = stat.getVersion();
                storage = Storage.SYNCED;
            } catch (KeeperException.NoNodeException e) {
                throw new CoordinateMissingException("Coordinate does not exist " + path);
            } catch (KeeperException e) {
                throw new CloudnameException("writeStatusEndpoint: " + e.getMessage(), e);
            } catch (UnsupportedEncodingException e) {
                throw new CloudnameException(e);
            } catch (InterruptedException e) {
                throw new CloudnameException(e);
            } catch (IOException e) {
                throw new CloudnameException(e);
            }
        }
    }
}