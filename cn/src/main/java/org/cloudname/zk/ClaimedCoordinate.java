package org.cloudname.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.cloudname.*;

import java.io.IOException;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * This class keeps track of coordinate data and endpoints for a coordinate. It is notified about the state
 * of ZooKeeper connection by implementing the ZkUserInterface. It implements the Watcher interface to
 * track the specific path of the coordinate. This is useful for being notified if something happens
 * to the coordinate (if it is overwritten etc).
 *
 * @author dybdahl
 */
public class ClaimedCoordinate implements Watcher, ZkObjectHandler.ConnectionStateChanged {

    public CloudnameLock getCloudnameLock(CloudnameLock.Scope scope, String lockName) {
        return new ZkCloudnameLock(zkClient.getZookeeper(), coordinate, scope, lockName);
    }

    private final AtomicBoolean isSynchronizedWithZooKeeper = new AtomicBoolean(false);

    /**
     * The client of the class has to call start. This will flip this bit.
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * The connection from client to ZooKeeper might go down. If it comes up again within some time window
     * the server might think an ephemeral node should be alive. The client lib might think otherwise.
     * If this flag is set, the time event should check version, content and session ID, if correct, delete
     * the node so it can be claimed in the normal way.
     */
    private AtomicBoolean checkVersion = new AtomicBoolean(false);

    /**
     * We keep track of the last version so we know if we are in sync.
     */
    private AtomicInteger lastStatusVersion = new AtomicInteger(-1);


    private static final Logger LOG = Logger.getLogger(ClaimedCoordinate.class.getName());


    private final ZkObjectHandler.Client zkClient;

    /**
     * The claimed coordinate.
     */
    private final Coordinate coordinate;

    /**
     * Status path of the coordinate.
     */
    private final String path;


    private final Object callbacksMonitor = new Object();

    /**
     * The endpoints and the status of the coordinate is stored here.
     */
    private final ZkCoordinateData zkCoordinateData = new ZkCoordinateData();

    /**
     * For running internal thread.
     */
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    /**
     * A list of the coordinate listeners that are registered for this coordinate.
     */
    private final List<CoordinateListener> coordinateListenerList =
            Collections.synchronizedList(new ArrayList<CoordinateListener>());

    /**
     * A list of tracked configs for this coordinate.
     */
    private final List<TrackedConfig> trackedConfigList =
            Collections.synchronizedList(new ArrayList<TrackedConfig>());

    /**
     * Constructor, the ZooKeeper instances is retrieved from implementing the ZkUserInterface so the object
     * is not ready to be used before the ZooKeeper instance is received.
     */
    public ClaimedCoordinate(Coordinate coordinate, ZkObjectHandler.Client zkClient) {
        this.coordinate = coordinate;
        path = ZkCoordinatePath.getStatusPath(coordinate);
        this.zkClient = zkClient;
    }

    /**
     * Claims a coordinate. To know if it was successful or not you need to register a listener.
     * @return this.
     */
    public ClaimedCoordinate start()  {
        started.set(true);
        final  long periodicDelayMs = 2000;
        scheduler.scheduleWithFixedDelay(new ResolveProblems(), 1 /* initial delay ms */,
                periodicDelayMs, TimeUnit.MILLISECONDS);
        return this;
    }

    @Override
    public void connectionUp() {
        isSynchronizedWithZooKeeper.set(false);
    }

    @Override
    public void connectionDown() {
        isSynchronizedWithZooKeeper.set(false);
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
            LOG.info("Claim callback with " + returnCode.name() + " " + claimedCoordinate.path + " synched: "
                    + isSynchronizedWithZooKeeper.get() + this);
            switch (returnCode) {
                // The tryClaim was successful. This means that the node was created. We need to populate the
                // status and endpoints.
                case OK:
                    synchronized (parent) {

                        // We should be the first one to write to the new node, or fail.
                        // This requires that the first version is 0, have not seen this documented but it should
                        // be a fair assumption and is verified by unit tests.
                        lastStatusVersion.set(0);

                        // We need to set this to synced or updateCoordinateData will complain.
                        // updateCoordinateData will set it to out-of-sync in case of problems.
                        isSynchronizedWithZooKeeper.set(true);

                        try {
                            claimedCoordinate.updateCoordinateData();
                            /*for (Object x : coordinateListenerList.toArray()) {
                                 CoordinateListener l = (CoordinateListener) x;
                                l.onCoordinateEvent(CoordinateListener.Event.COORDINATE_OK, "claim ok");

                            } */
                        } catch (CoordinateMissingException e) {
                            LOG.fine("Problems writing config, coordinate missing.");
                            claimedCoordinate.sendEventToCoordinateListener(
                                    CoordinateListener.Event.NOT_OWNER,
                                    "Can not write config after claim: " + returnCode.name());
                            return;
                        } catch (CloudnameException e) {
                            LOG.fine("Problems writing config." + e.getMessage());
                            claimedCoordinate.sendEventToCoordinateListener(
                                    CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                                    "Can not write config after claim: " + returnCode.name());
                            return;
                        }
                    }

                    try {
                        registerWatcher();
                    } catch (CloudnameException e) {
                        LOG.fine("Failed register watcher after claim. Going to state out of sync: " + e.getMessage());
                        synchronized (this) {
                            isSynchronizedWithZooKeeper.set(false);
                            return;
                        }
                    } catch (InterruptedException e) {
                        synchronized (this) {
                            LOG.fine("Interrupted while setting up new watcher. Going to state out of sync.");
                            isSynchronizedWithZooKeeper.set(false);
                            return;
                        }
                    }
                    // No exceptions, let's celebrate with a log message.
                    LOG.info("Claimed processed ok, path: " + path);
                    claimedCoordinate.sendEventToCoordinateListener(CoordinateListener.Event.COORDINATE_OK, "claimed");
                    return;

                case NODEEXISTS:
                    // Someone has already claimed the coordinate. It might have been us in a different thread.
                    // If we already have claimed the coordinate then don't care. Else notify the client.
                    //synchronized (parent) {
                        // If everything is fine, this is not a true negative, so ignore it. It might happen if
                        // two attempts to tryClaim the coordinate run in parallel.
                        if (isSynchronizedWithZooKeeper.get() && started.get()) {
                            LOG.info("Everything is fine, ignoring NODEEXISTS message, path: " + path);
                            return;
                        }
                    //}
                    LOG.info("Claimed fail, node already exists and probably not by us, path: " + path);
                    claimedCoordinate.sendEventToCoordinateListener(
                            CoordinateListener.Event.NOT_OWNER, "Node already exists.");
                    LOG.info("isSynchronizedWithZooKeeper: " + isSynchronizedWithZooKeeper.get());
                    checkVersion.set(true);
                    return;
                case NONODE:
                    LOG.info("Could not claim due to missing coordinate, path: " + path);
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


    class ResolveProblems implements Runnable {
        @Override
        public void run() {
            if (isSynchronizedWithZooKeeper.get() ||  ! zkClient.isConnected() ||
                    ! started.get()) {

                return;
            }
            if (checkVersion.getAndSet(false)) {
                try {
                    Stat stat = zkClient.getZookeeper().exists(path, null);
                    if (stat != null && zkClient.getZookeeper().getSessionId() == stat.getEphemeralOwner()) {
                        zkClient.getZookeeper().delete(path, lastStatusVersion.get());
                    } else {
                    }
                } catch (InterruptedException e) {
                    LOG.info("Interruppted");
                    checkVersion.set(true);
                } catch (KeeperException e) {
                    LOG.info("exception "+ e.getMessage());
                    checkVersion.set(true);
                }

            }
            LOG.info("We are out-of-sync, have a zookeeper connection, and are started, trying reclaim: " + path + this);
            tryClaim();
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
     * Adds new endpoints and persist them. Requires that this instance owns the tryClaim to the coordinate.
     * @param newEndpoints endpoints to be added.
     */
    public void putEndpoints(List<Endpoint> newEndpoints) throws CloudnameException, CoordinateMissingException {
        zkCoordinateData.putEndpoints(newEndpoints);
        updateCoordinateData();
    }

    /**
     * Remove endpoints and persist it. Requires that this instance owns the tryClaim to the coordinate.
     * @param names names of endpoints to be removed.
     */
    public void removeEndpoints(List<String> names) throws CloudnameException, CoordinateMissingException {
        zkCoordinateData.removeEndpoints(names);
        updateCoordinateData();
    }

    /**
     * Release the tryClaim of the coordinate. It means that nobody owns the coordinate anymore.
     * Requires that that this instance owns the tryClaim to the coordinate.
     */
    public void releaseClaim() throws CloudnameException {
        scheduler.shutdown();

        try {
            zkClient.getZookeeper().delete(path, lastStatusVersion.get());
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        }
        lastStatusVersion.set(-1);
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
        synchronized (callbacksMonitor) {
            coordinateListenerList.add(coordinateListener);
            if (isSynchronizedWithZooKeeper.get()) {
                coordinateListener.onCoordinateEvent(CoordinateListener.Event.COORDINATE_OK, message);
            } else {
                sendEventToCoordinateListener(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE, "Not ok ");
            }
        }
    }


    /**
     * Registers a configlistener that will receive events when there are changes to the config node.
     * Don't do any heavy lifting in the callback and don't call cloudname from the callback as this might create
     * a deadlock.
     * @param trackedConfig
     */
    public void registerTrackedConfig(TrackedConfig trackedConfig)  {
        trackedConfigList.add(trackedConfig);
    }

    /**
     * Handles event from ZooKeeper for this coordinate.
     * @param event
     */
    @Override public void process(WatchedEvent event) {
        LOG.fine("Got an event from ZooKeeper " + event.toString());

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
                        isSynchronizedWithZooKeeper.set(false);
                        sendEventToCoordinateListener(CoordinateListener.Event.NO_CONNECTION_TO_STORAGE,
                                event.toString());
                        return;
                }
                return;

            case NodeDeleted:
                // If node is deleted, we have no node to place a new watcher so we stop watching.
                isSynchronizedWithZooKeeper.set(false);
                sendEventToCoordinateListener(CoordinateListener.Event.NOT_OWNER, event.toString());
                return;

            case NodeDataChanged:
                LOG.fine("Node data changed, check versions.");
                synchronized (this) {
                    try {
                        Stat stat = zkClient.getZookeeper().exists(path, this);
                        if (stat == null) {
                            LOG.fine("Could not stat path, setting out of synch, will retry claim.");
                            isSynchronizedWithZooKeeper.set(false);
                        } else {
                            LOG.fine("Previous version is " + lastStatusVersion.get() + " now is " + stat.getVersion());
                            if (stat.getVersion() != lastStatusVersion.get()) {
                                LOG.info("Version mismatch, sending out of sync.");
                                isSynchronizedWithZooKeeper.set(false);
                            }
                        }
                    } catch (KeeperException e) {
                        LOG.fine("Problems with zookeeper, sending consistencyState out of sync: " + e.getMessage());
                        isSynchronizedWithZooKeeper.set(false);
                    } catch (InterruptedException e) {
                        LOG.fine("Got interrupted: " + e.getMessage());
                        isSynchronizedWithZooKeeper.set(false);
                        return;
                    }

                }
                if (! isSynchronizedWithZooKeeper.get()) {
                    sendEventToCoordinateListener(CoordinateListener.Event.COORDINATE_OUT_OF_SYNC, event.toString());
                }
                return;

            case NodeChildrenChanged:
            case NodeCreated:
                // This should not happen..
                isSynchronizedWithZooKeeper.set(false);
                sendEventToCoordinateListener(CoordinateListener.Event.COORDINATE_OUT_OF_SYNC, event.toString());
                return;
        }
    }


    private void tryClaim() {
        try {
            zkClient.getZookeeper().create(
                    path, zkCoordinateData.snapshot().serialize().getBytes(Util.CHARSET_NAME),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new ClaimCallback(), this);
        } catch (IOException e) {
            LOG.info("Got IO exception on claim with new ZooKeeper instance " + e.getMessage());
        }
    }


    /**
     * Sends an event too all coordinate listeners. Note that the event is sent from this thread so if the
     * callback code does the wrong calls, deadlocks might occur.
     * @param event
     * @param message
     */
    private void sendEventToCoordinateListener(CoordinateListener.Event event, String message) {
        synchronized (callbacksMonitor) {
            LOG.fine("Event " + event.name() + " " + message);
            for (CoordinateListener listener : coordinateListenerList) {
                listener.onCoordinateEvent(event, message);
            }
        }
    }

    /**
     * Register a watcher for the coordinate.
     */
    private void registerWatcher() throws CloudnameException, InterruptedException {
        LOG.fine("Register watcher for ZooKeeper..");
        try {
            zkClient.getZookeeper().exists(path, this);
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

            if (! started.get()) {
                throw new IllegalStateException("Not started.");
            }

            if (! zkClient.isConnected()) {
                throw new CloudnameException("No proper connection with zookeeper.");
            }

            try {
                Stat stat = zkClient.getZookeeper().setData(path,
                        zkCoordinateData.snapshot().serialize().getBytes(Util.CHARSET_NAME),
                        lastStatusVersion.get());
                lastStatusVersion.set(stat.getVersion());
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
