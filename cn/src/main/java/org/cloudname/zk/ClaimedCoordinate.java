package org.cloudname.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.cloudname.*;

import java.io.IOException;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.logging.Logger;

/**
 * This class keeps track of coordinate data and endpoints for a coordinate. It is notified about the state
 * of ZooKeeper connection by implementing the ZkUserInterface. It implements the Watcher interface to
 * track the specific path of the coordinate. This is useful for being notified if something happens
 * to the coordinate (if it is overwritten etc).
 *
 * @author dybdahl
 */
public class ClaimedCoordinate implements Watcher, ZkUserInterface {

    /**
     * The consistencyState is either OUT OF SYNC or SYNCED. It is SYNCED if the current data model in memory
     * is the same as the model in ZooKeeper.
     */
    private enum ConsistencyState {
        OUT_OF_SYNC,
        SYNCED,
    }

    /**
     * We always start out of sync.
     */
    private ConsistencyState consistencyState = ConsistencyState.OUT_OF_SYNC;

    /**
     * The client of the class has to call start. This will flip this bit.
     */
    private boolean started = false;

    /**
     * We keep track of the last version so we know if we are in sync.
     */
    private int lastStatusVersion = -1;

    private static final Logger log = Logger.getLogger(ClaimedCoordinate.class.getName());

    /**
     * The ZooKeeper instance we use. This is a dynamic variable and can be changed by functions in the ZkUserInterface.
     */
    private ZooKeeper zk;

    /**
     * Status path of the coordinate.
     */
    private final String path;

    /**
     * The endpoints and the status of the coordinate is stored here.
     */
    private ZkCoordinateData zkCoordinateData = new ZkCoordinateData();

    /**
     * A list of the coordinate listeners that are registered for this coordinate.
     */
    private List<CoordinateListener> coordinateListenerList =
            Collections.synchronizedList(new ArrayList<CoordinateListener>());


    /**
     * Constructor, the ZooKeeper instances is retrieved from implementing the ZkUserInterface so the object
     * is not ready to be used before the ZooKeeper instance is received.
     * @param path is the path of the status of the coordinate.
     */
    public ClaimedCoordinate(String path) {
        this.path = path;
    }

    /**
     * Claims a coordinate. To know if it was successful or not you need to register a listener.
     * @return this.
     */
    public ClaimedCoordinate start()  {
        synchronized (this) {
            started = true;
        }
        timeEvent();
        return this;
    }

    /**
     * This is implementing part of ZkUserInterface.
     */
    @Override
    public void zooKeeperDown() {
        log.fine("ClaimedCoordinate: Got event ZooKeeper is down, path: " + path);
        synchronized (this) {
            zk = null;
            consistencyState = ConsistencyState.OUT_OF_SYNC;
        }
        sendEventToCoordinateListener(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                "Got message from parent watcher.");
    }
    
    /**
     * This is implementing part of ZkUserInterface.
     */
    @Override
    public void newZooKeeperInstance(ZooKeeper zk) {
        log.fine("ClaimedCoordinate: Got new ZeeKeeper, starting potential cleanup, path: " + path);
        synchronized (this) {
            this.zk = zk;
            // We always start by assuming it is unclaimed.
            consistencyState = ConsistencyState.OUT_OF_SYNC;

            if (! started) {
                return;
            }
            log.fine("ClaimedCoordinate: Reclaiming coordinate due to new zookeeper.");
            claim(zk);
        }
    }
    
    /**
     * This class implements the logic for handling callbacks from ZooKeeper on claim.
     * In general we could just ignore errors since we have a time based retry mechanism. However,
     * we want to notify clients, and we need to update the consistencyState.
     */
    class ClaimCallback implements AsyncCallback.StringCallback {

        @Override
        public void processResult(int rawReturnCode, String notUsed, Object parent, String notUsed2) {

            KeeperException.Code returnCode =  KeeperException.Code.get(rawReturnCode);
            ClaimedCoordinate claimedCoordinate = (ClaimedCoordinate) parent;
            log.fine("Claim callback with " + returnCode.name() + " " + claimedCoordinate.path + " "
                    + consistencyState.name());
            switch (returnCode) {
                // The claim was successful. This means that the node was created. We need to populate the
                // status and endpoints.
                case OK:
                    synchronized (parent) {

                        // We should be the first one to write to the new node, or fail.
                        // This requires that the first version is 0, have not seen this documented but it should
                        // be a fair assumption and is verified by unit tests.
                        lastStatusVersion = 0;
                        // We need to set this to synced or updateCoordinateData will complain.
                        // updateCoordinateData will set it to out-of-sync in case of problems.
                        consistencyState = ConsistencyState.SYNCED;

                        try {
                            claimedCoordinate.updateCoordinateData();
                        } catch (CoordinateMissingException e) {
                            log.fine("Problems writing config, coordinate missing.");
                            claimedCoordinate.sendEventToCoordinateListener(
                                    CoordinateListener.Event.NOT_OWNER,
                                    "Can not write config after claim: " + returnCode.name());
                            return;
                        } catch (CloudnameException e) {
                            log.fine("Problems writing config." + e.getMessage());
                            claimedCoordinate.sendEventToCoordinateListener(
                                    CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                                    "Can not write config after claim: " + returnCode.name());
                            return;
                        }
                    }

                    try {
                        registerWatcher();
                    } catch (CloudnameException e) {
                        log.fine("Failed register watcher after claim. Going to state out of sync: " + e.getMessage());
                        synchronized (this) {
                            consistencyState = ConsistencyState.OUT_OF_SYNC;
                            return;
                        }
                    } catch (InterruptedException e) {
                        synchronized (this) {
                            log.fine("Interrupted while setting up new watcher. Going to state out of sync.");
                            consistencyState = ConsistencyState.OUT_OF_SYNC;
                            return;
                        }
                    }
                    // No exceptions, let's celebrate with a log message.
                    log.info("Claimed processed ok, path: " + path);
                    claimedCoordinate.sendEventToCoordinateListener(CoordinateListener.Event.COORDINATE_OK, "claimed");
                    return;
                
                case NODEEXISTS:
                    // Someone has already claimed the coordinate. It might have been us in a different thread.
                    // If we already have claimed the coordinate then don't care. Else notify the client.
                    synchronized (parent) {
                        // If everything is fine, this is not a true negative, so ignore it. It might happen if
                        // two attempts to claim the coordinate run in parallel.
                        if (consistencyState == ConsistencyState.SYNCED && started) {
                            log.fine("Everything is fine, ignoring NODEEXISTS message, path: " + path);
                            return;
                        }
                    }
                    log.info("Claimed fail, node already exists and probably not by us, path: " + path);
                    claimedCoordinate.sendEventToCoordinateListener(
                            CoordinateListener.Event.NOT_OWNER, "Node already exists.");
                    return;
                case NONODE:
                    log.info("Could not claim due to missing coordinate, path: " + path);
                    claimedCoordinate.sendEventToCoordinateListener(
                            CoordinateListener.Event.NOT_OWNER,
                            "No node on claiming coordinate: " + returnCode.name());
                    return;

                default:
                    // Random problem, report the problem to the client.
                    claimedCoordinate.sendEventToCoordinateListener(
                            CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                            "Could not reclaim coordinate. Return code: " + returnCode.name());
                    return;
            }
        }
    }

    /**
     * This function is called externally now and then to let the object check it's state and try to resolve problems.
     */
    @Override
    public void timeEvent() {
        synchronized (this) {
            if (consistencyState == ConsistencyState.SYNCED || zk == null || ! started) {
                return;
            }
            log.fine("We are out-of-sync, have a zookeeper connection, and are started, trying reclaim: " + path);
            claim(zk);
        }
    }


    /**
     * Updates the ServiceStatus and persists it. Only allowed if we claimed the coordinate.
     * @param status The new value for serviceStatus.
     */
    public void updateStatus(ServiceStatus status) throws CloudnameException, CoordinateMissingException {
        zkCoordinateData.setStatus(status);
        updateCoordinateData();
    }

    /**
     * Adds new endpoints and persist them. Requires that this instance owns the claim to the coordinate.
     * @param newEndpoints endpoints to be added.
     */
    public void putEndpoints(List<Endpoint> newEndpoints) throws CloudnameException, CoordinateMissingException {
        zkCoordinateData.putEndpoints(newEndpoints);
        updateCoordinateData();
    }

    /**
     * Remove endpoints and persist it. Requires that this instance owns the claim to the coordinate.
     * @param names names of endpoints to be removed.
     */
    public void removeEndpoints(List<String> names) throws CloudnameException, CoordinateMissingException {
        zkCoordinateData.removeEndpoints(names);
        updateCoordinateData();
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
            zkCoordinateData = null;
            lastStatusVersion = -1;
        }
    }

    /**
     * Creates a string for debugging etc
     * @return serialized version of the instance data.
     */
    public synchronized String toString() {
       return zkCoordinateData.snapshot().toString();
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
            if (consistencyState == ConsistencyState.SYNCED) {
                coordinateListener.onCoordinateEvent(CoordinateListener.Event.COORDINATE_OK, message);
            } else {
                sendEventToCoordinateListener(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE, "Not ok " +
                        consistencyState.toString());
            }
        }
    }

    /**
     * Handles event from ZooKeeper for this coordinate.
     * @param event
     */
    @Override public void process(WatchedEvent event) {
        log.fine("Got an event from ZooKeeper " + event.toString());

        switch (event.getType()) {

            case None:
                switch (event.getState()) {
                    case SyncConnected:
                        break;
                    case Disconnected:
                    case AuthFailed:
                    case Expired:
                    default:
                        // If we lost connection, we don't attempt to register another watcher as this might be
                        // blocking forever. Parent will try to reconnect (reclaim) later.
                        synchronized (this) {
                            consistencyState = ConsistencyState.OUT_OF_SYNC;
                        }
                        sendEventToCoordinateListener(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                                event.toString());
                        return;
                }
                return;

            case NodeDeleted:
                // If node is deleted, we have no node to place a new watcher so we stop watching.
                synchronized (this) {
                    consistencyState = ConsistencyState.OUT_OF_SYNC;
                }
                sendEventToCoordinateListener(CoordinateListener.Event.NOT_OWNER, event.toString());
                return;

            case NodeDataChanged:
                log.fine("Node data changed, check versions.");
                synchronized (this) {
                    try {
                        Stat stat = getZooKeeper().exists(path, this);
                        log.fine("Previous version is " + lastStatusVersion + " now is " + stat.getVersion());
                        if (stat.getVersion() != lastStatusVersion) {
                            log.info("Version mismatch, sending out of sync.");
                            consistencyState = ConsistencyState.OUT_OF_SYNC;
                        }
                    } catch (KeeperException e) {
                        log.fine("Problems with zookeeper, sending consistencyState out of sync: " + e.getMessage());
                        consistencyState = ConsistencyState.OUT_OF_SYNC;
                    } catch (InterruptedException e) {
                        log.fine("Got interrupted: " + e.getMessage());
                        consistencyState = ConsistencyState.OUT_OF_SYNC;
                        return;
                    }

                }
                if (consistencyState == ConsistencyState.OUT_OF_SYNC) {
                    sendEventToCoordinateListener(CoordinateListener.Event.COORDINATE_OUT_OF_SYNC, event.toString());
                }
                return;

            case NodeChildrenChanged:
            case NodeCreated:
                // This should not happen..
                synchronized (this) {
                    consistencyState = ConsistencyState.OUT_OF_SYNC;
                }
                sendEventToCoordinateListener(CoordinateListener.Event.COORDINATE_OUT_OF_SYNC, event.toString());
                return;
        }
    }

    /**
     * This does not synchronize internally so it is safe to call it from synchronized(this) code (for simplicity).
     * @param zkArg we pass this parameter to avoid locking in this code.
     */
    private void claim(ZooKeeper zkArg) {
        try {
            zkArg.create(
                    path, zkCoordinateData.snapshot().serialize().getBytes(Util.CHARSET_NAME),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new ClaimCallback(), this);
        } catch (IOException e) {
            // We don't care about this, the system will try to reclaim later.
            log.info("Got IO exception on claim with new ZooKeeper instance " + e.getMessage());
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
    private void sendEventToCoordinateListener(CoordinateListener.Event event, String message) {
        synchronized (this) {
            log.fine("Event " + event.name() + " " + message);
            for (CoordinateListener listener : coordinateListenerList) {
                listener.onCoordinateEvent(event, message);
            }
        }
    }

    /**
     * Register a watcher for the coordinate.
     */
    private void registerWatcher() throws CloudnameException, InterruptedException {
        log.fine("Register watcher for ZooKeeper..");
        try {
            getZooKeeper().exists(path, this);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        }
    }

    /**
     * Creates the serialized value of the object and stores this in ZooKeeper under the path.
     * It updates the lastStatusVersion. It does not set a watcher for the path.
     */
    private void updateCoordinateData() throws CoordinateMissingException, CloudnameException {
        synchronized (this) {

            if (! started) {
                throw new IllegalStateException("Not started yet: " + consistencyState.name());
            }
            
            if (consistencyState == ConsistencyState.OUT_OF_SYNC) {
                throw new CloudnameException("No proper connection with zookeeper.");
            }

            try {
                Stat stat = getZooKeeper().setData(path,
                        zkCoordinateData.snapshot().serialize().getBytes(Util.CHARSET_NAME),
                        lastStatusVersion);
                lastStatusVersion = stat.getVersion();
            } catch (KeeperException.NoNodeException e) {
                throw new CoordinateMissingException("Coordinate does not exist " + path);
            } catch (KeeperException e) {
                throw new CloudnameException("ZooKeeper errror in updateCoordinateData: " + e.getMessage(), e);
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