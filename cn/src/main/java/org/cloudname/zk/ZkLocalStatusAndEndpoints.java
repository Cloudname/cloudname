package org.cloudname.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.cloudname.*;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
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
public class ZkLocalStatusAndEndpoints implements Watcher, ZkUserInterface {

    private Storage storage = Storage.OUT_OF_SYNC;
    private boolean started = false;

    private int lastStatusVersion = -1;
    private static final Logger log = Logger.getLogger(ZkLocalStatusAndEndpoints.class.getName());
    private ZooKeeper zk;
    private final String path;
    //final CoordinateListener coordinateListener;

    private LocalStatusAndEndpoints localStatusAndEndpoints = new LocalStatusAndEndpoints();
    private List<CoordinateListener> coordinateListenerList =
            Collections.synchronizedList(new ArrayList<CoordinateListener>());

    //CountDownLatch claimLatch = new CountDownLatch(1);

    /**
     * Constructor, the ZooKeeper instances is retrieved from implementing the ZkUserInterface so the object
     * is not ready to be used before the ZooKeeper instance is received.
     * @param path is the path of the status of the coordinate.
     */
    public ZkLocalStatusAndEndpoints(String path) {
        this.path = path;
    }


    @Override
    public void zooKeeperDown() {
        log.info("ZkLocalStatusAndEndpoints: Got event ZooKeeper is down.");
        synchronized (this) {
            zk = null;
            storage = Storage.OUT_OF_SYNC;
        }
        updateCoordinateListenersAndTakeAction(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE, "Got message from parent watcher.");
    }

    class ClaimCallback implements AsyncCallback.StringCallback {

        @Override
        public void processResult(int rawReturnCode, String s, Object parent, String s1) {

            KeeperException.Code returnCode =  KeeperException.Code.get(rawReturnCode);
            ZkLocalStatusAndEndpoints statusAndEndpoints =  (ZkLocalStatusAndEndpoints) parent;
            log.info("Claim callback " + returnCode.name() + " " + s);
           
            switch (returnCode) {

                case OK:
                    synchronized (statusAndEndpoints) {
                        try {
                            lastStatusVersion = -1;
                            statusAndEndpoints.writeStatusEndpoint();
                            storage = Storage.SYNCED;
                            log.info("Now in lock!!!!!!!");

                        } catch (CoordinateMissingException e) {
                            log.info("Problems writing config, coordinate missing");
                            statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                                    CoordinateListener.Event.NOT_OWNER,
                                    "Can not write config: " + returnCode.name() + " " + s);
                            break;
                        } catch (CloudnameException e) {
                            log.info("Problems writing config." + e.getMessage());
                            statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                                    CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE,
                                    "Can not write config after claim: " + returnCode.name() + " " + s);
                            break;
                        }
                    }

                    try {
                        registerWatcher();
                    } catch (CloudnameException e) {
                        log.info("Interrupted while setting up new watcher. Going to state out of sync.");
                        storage = Storage.OUT_OF_SYNC;

                    } catch (InterruptedException e) {
                        log.info("Interrupted while setting up new watcher. Going to state out of sync.");
                        storage = Storage.OUT_OF_SYNC;
                    }
                    log.info("Now in lock!!!!!!!  111111");
                    statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                            CoordinateListener.Event.COORDINATE_OK, "claimed");
                    log.info("Now in lock!!!!!!!  2222222");
                    break;
                case NODEEXISTS:
                    synchronized (statusAndEndpoints) {
                        // If everything is fine, this is not a true negative, so ignore it.
                        if (storage == Storage.SYNCED && started) {
                            break;
                        }
                    }
                    statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                            CoordinateListener.Event.NOT_OWNER, "Node already exists.");
                    break;
                case NONODE:
                    statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                            CoordinateListener.Event.NOT_OWNER,
                            "No node on claiming coordinate: " + returnCode.name() + " " + s);
                    break;
                
                default:
                     statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                             CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE,
                             "Could not reclaim coordinate. Return code: " + returnCode.name() + " " + s);

            }

        }
    }

    private void claim(ZooKeeper zkArg) {

        try {
            zkArg.create(
                    path, localStatusAndEndpoints.serialize().getBytes(Util.CHARSET_NAME),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new ClaimCallback(), this);
        } catch (CloudnameException e) {
            log.info("Could not claim with the new ZooKeeper instance: " + e.getMessage());
        } catch (IOException e) {
            log.info("Got IO exception on claim with new ZooKeeper instance " + e.getMessage());
        }
    }

    
    @Override
    public void newZooKeeperInstance(ZooKeeper zk) {
        log.info("ZkLocalStatusAndEndpoints: Got new ZeeKeeper, expect session to be down so starting potential cleanup." + zk.getSessionId());
        synchronized (this) {
            this.zk = zk;
            // We always start by assuming it is unclaimed.
            storage = Storage.OUT_OF_SYNC;

            if (! started) {
                return;
            }
            log.info("ZkLocalStatusAndEndpoints: Reclaiming coordinate.");
            claim(zk);
        }
    }

    @Override
    public void wakeUp() {
        
        synchronized (this) {
            if ( storage == Storage.SYNCED || zk == null || ! started) {
                return;
            }
            claim(zk);
        }
        System.out.println("alive");
    }

    private enum Storage {
        OUT_OF_SYNC,
        SYNCED,
    }


    /**
     * Updates the ServiceStatus and persists it. Only allowed if we claimed the coordinate.
     * @param status The new value for serviceStatus.
     */
    public void updateStatus(ServiceStatus status) throws CloudnameException, CoordinateMissingException {
        localStatusAndEndpoints.updateStatus(status);
        writeStatusEndpoint();
    }



    /**
     * Adds new endpoints and persist them. Requires that this instance owns the claim to the coordinate.
     * @param newEndpoints endpoints to be added.
     */
    public void putEndpoints(List<Endpoint> newEndpoints)
            throws EndpointException, CloudnameException, CoordinateMissingException {
        localStatusAndEndpoints.putEndpoints(newEndpoints);
        writeStatusEndpoint();
    }

    /**
     * Remove endpoints and persist it. Requires that this instance owns the claim to the coordinate.
     * @param names names of endpoints to be removed.
     */
    public void removeEndpoints(List<String> names)
            throws EndpointException, CloudnameException, CoordinateMissingException {
        localStatusAndEndpoints.removeEndpoints(names);
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
            localStatusAndEndpoints = null;
            lastStatusVersion = -1;
        }
    }

    /**
     * Creates a string for debugging etc
     * @return serialized version of the instance data.
     */
    public synchronized String toString() {
       return localStatusAndEndpoints.toString();
    }

    /**
     * Registers a coordinatelistener that will receive events when there are changes to the status node.
     * Don't do any heavy lifting in the callback and don't call cloudname from the callback as this might create
     * a deadlock.
     * @param coordinateListener
     */
    public void registerCoordinateListener(CoordinateListener coordinateListener) throws CloudnameException {

        String message = "New listener added, resending current state.";
        System.out.println(message + "!!!!!!!!!!" + storage.toString());
        synchronized (this) {
            coordinateListenerList.add(coordinateListener);
            if (storage == Storage.SYNCED) {
                coordinateListener.onConfigEvent(CoordinateListener.Event.COORDINATE_OK, message);
            } else {
                updateCoordinateListenersAndTakeAction(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE, "Not ok " +
                        storage.toString());
            }
        }
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
            updateCoordinateListenersAndTakeAction(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE, event.toString());
            return;
        }
        if (event.getType() == Event.EventType.None &&
                (event.getState() == Event.KeeperState.Expired)) {
            updateCoordinateListenersAndTakeAction(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE, event.toString());
            return;
        }

        // If node is deleted, we have no node to place a new watcher so we stop watching.
        if (event.getType() == Event.EventType.NodeDeleted) {
            updateCoordinateListenersAndTakeAction(CoordinateListener.Event.NOT_OWNER, event.toString());
            return;
        }

        if (event.getType() == Event.EventType.NodeDataChanged) {

            // todo only true for claimed coordinates

            updateCoordinateListenersAndTakeAction(CoordinateListener.Event.COORDINATE_OUT_OF_SYNC, event.toString());
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
            updateCoordinateListenersAndTakeAction(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE,
                    "Failed setting up new watcher, CloudnameException.");
            return;
        } catch (InterruptedException e) {
            updateCoordinateListenersAndTakeAction(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE,
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
    private void updateCoordinateListenersAndTakeAction(CoordinateListener.Event event, String message) {
        synchronized (this) {
            log.info("Event " + event.name() + " " + message);
            //coordinateListener.onConfigEvent(event, message);
            for (CoordinateListener listener : coordinateListenerList) {
                listener.onConfigEvent(event, message);
            }

            // Simply ignore the action from the listener, we are ok anyway.
            if (event == CoordinateListener.Event.COORDINATE_OK) {
                storage = Storage.SYNCED;
                return;
            }

            if (event == CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE) {
                storage = Storage.OUT_OF_SYNC;
                return;
            }

            if (event == CoordinateListener.Event.NOT_OWNER) {
                storage = Storage.OUT_OF_SYNC;
                try {
                    registerWatcher();
                } catch (CloudnameException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    log.info("failed setting up watcher, giving up");
                    return;
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    log.info("failed setting up watcher, giving up");
                    return;
                }
                return;
            }

            if (event == CoordinateListener.Event.COORDINATE_OUT_OF_SYNC) {

                    try {
                        writeStatusEndpoint();
                    } catch (CoordinateMissingException e) {
                        storage = Storage.OUT_OF_SYNC;
                        return;
                    } catch (CloudnameException e) {
                        storage = Storage.OUT_OF_SYNC;
                        log.info("Exceptiion cloudname" + e.getMessage());
                        return;
                    }

                return;

            }
            storage = Storage.OUT_OF_SYNC;
        }
        System.err.println("ZkLocalStatusAndEndpoints trying to recover.");
        //newZooKeeperInstance(getZooKeeper());
    }

    /*public void waitForSynchronized() throws InterruptedException {
        synchronized (this) {
            while (storage != Storage.SYNCED) {
                storage.wait();
            }
        }
    } */

    /**
     * Claims a coordinate.
     * @return this.
     */
    public ZkLocalStatusAndEndpoints start()  {
        synchronized (this) {
            started = true;
        }
        wakeUp();

      //  claimLatch.await();
       /* }

        try {
            started = true;
            
            synchronized (this) {
                getZooKeeper().create(
                        path, localStatusAndEndpoints.serialize().getBytes(Util.CHARSET_NAME),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                storage = Storage.SYNCED;
            }

        } catch (KeeperException.NodeExistsException e) {
            log.info("Coordinate already claimed  (" + path + ")");

            throw new CoordinateAlreadyClaimedException("Coordinate already claimed.(" + path + ")");
        } catch (KeeperException.NoNodeException e) {
            log.info("Coordinate does not exist  (" + path + ")");

            throw new CoordinateMissingException("Coordinate does not exist  (" + path + ")");
        } catch (KeeperException e) {
            log.log(Level.SEVERE, "Problems with claiming" + e.getMessage());

            throw new CloudnameException(e);
        } catch (UnsupportedEncodingException e) {
            log.info("Claim: This is not supposed to be happening since CHARSET_NAME " +
               " should always be UTF-8.");

            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            log.info("claim interrupped");

            throw new CloudnameException(e);
        } catch (IOException e) {
            log.info("claim interrupped");

            throw new CloudnameException(e);
        }
        log.info("Claimed coordinate");

        // Stat the serviceStatus node so we have the version.  If later
        // we try to operate on the serviceStatus node and we do not have
        // the correct version this can mean that someone else has
        // been meddling with the serviceStatus node.  In which case we
        // must complain loudly.
        try {
            // TODO(borud, dybdahl): Consider if we need to re-read the content of the zookeeper. Can it
            // possible change between create and exists? Can we use version number to detect this or is that
            // depending on implementation details of ZooKeeper?
            Stat stat = getZooKeeper().exists(path, this);
            lastStatusVersion = stat.getVersion();
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }

         */
        return this;
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
                        localStatusAndEndpoints.serialize().getBytes(Util.CHARSET_NAME),
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