package org.cloudname.zk;

import org.apache.zookeeper.*;
import org.cloudname.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class keeps track of serviceStatus and endpoints for a coordinate.
 *
 * @author dybdahl
 */
public class TrackedCoordinate implements Watcher, ZkObjectHandler.ConnectionStateChanged {


    /**
     * The client can implement this to get notified on changes.
     */
    public interface ExpressionResolverNotify {
        void stateChanged(final String statusPath);
    }

    private ZkCoordinateData.Snapshot coordinateData = null;
    private final Object coordinateDataMonitor = new Object();

    private static final Logger LOG = Logger.getLogger(TrackedCoordinate.class.getName());
    private final String path;
    private final ExpressionResolverNotify client;
    private final AtomicBoolean isSynchronizedWithZookeeper = new AtomicBoolean(false);
    private final ZkObjectHandler.Client zkClient;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    private final CountDownLatch firstRound = new CountDownLatch(1);
    /**
     * Constructor, the ZooKeeper instances is retrieved from implementing the ZkUserInterface so
     * the object is not ready to be used before the ZooKeeper instance is received.
     * @param path is the path of the status of the coordinate.
     */
    public TrackedCoordinate(
            final ExpressionResolverNotify client, final String path,
            final ZkObjectHandler.Client zkClient) {
        LOG.finest("Tracking coordinate with path " + path);
        this.path = path;
        this.client = client;
        this.zkClient = zkClient;
    }

    // Implementation of ZkObjectHandler.ConnectionStateChanged.
    @Override
    public void connectionUp() {
    }

    // Implementation of ZkObjectHandler.ConnectionStateChanged.
    @Override
    public void connectionDown() {
        isSynchronizedWithZookeeper.set(false);
    }

    @Override
    public void shutDown() {
        stop();
    }

    /**
     * Signalize that the class should reload its data.
     */
    public void refreshAsync() {
        isSynchronizedWithZookeeper.set(false);
    }

    public void start() {
        zkClient.registerListener(this);
        final  long periodicDelayMs = 2000;
        scheduler.scheduleWithFixedDelay(new ReloadCoordinateData(), 1 /* initial delay ms */,
                periodicDelayMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdown();
        zkClient.deregisterListener(this);
    }

    public void waitForFirstData() throws InterruptedException {
        firstRound.await();
    }



    /**
     * This class handles reloading new data from zookeeper if we are out of synch.
     */
    class ReloadCoordinateData implements Runnable {
        @Override
        public void run() {
            if (! isSynchronizedWithZookeeper.getAndSet(true)) {  return;  }
            try {
                refreshCoordinateData();
            } catch (CloudnameException e) {
                LOG.log(Level.INFO, "exception on reloading coordinate data.", e);
                isSynchronizedWithZookeeper.set(false);
            }
            firstRound.countDown();
        }
    }


    public ZkCoordinateData.Snapshot getCoordinatedata() {
        synchronized (coordinateDataMonitor) {
            return coordinateData;
        }
    }


    /**
     * Creates a string for debugging etc
     * @return serialized version of the instance data.
     */
    public String toString() {
        synchronized (coordinateDataMonitor) {
            return coordinateData.toString();
        }
    }


    /**
     * Handles event from ZooKeeper for this coordinate.
     * @param event Event to handle
     */
    @Override public void process(WatchedEvent event) {
        LOG.fine("Got an event from ZooKeeper " + event.toString() + " path: " + path);

        switch (event.getType()) {
            case None:
                switch (event.getState()) {
                    case SyncConnected:
                        break;
                    case Disconnected:
                    case AuthFailed:
                    case Expired:
                    default:
                        // If we lost connection, we don't attempt to register another watcher as
                        // this might be blocking forever. Parent might try to reconnect.
                        return;
                }
                break;
            case NodeDeleted:
                synchronized (coordinateDataMonitor) {
                    coordinateData = new ZkCoordinateData().snapshot();
                }
                client.stateChanged(path);
                return;
            case NodeDataChanged:
                isSynchronizedWithZookeeper.set(false);
                return;
            case NodeChildrenChanged:
            case NodeCreated:
                break;
        }
        try {
            registerWatcher();
        } catch (CloudnameException e) {
            LOG.log(Level.INFO, "Got cloudname exception.", e);
        } catch (InterruptedException e) {
            LOG.log(Level.INFO, "Got interrupted exception.", e);
        }
    }


    /**
     * Loads the coordinate from ZooKeeper. In case of failure, we keep the old data.
     * Notifies the client if state changes.
     */
    private void refreshCoordinateData() throws CloudnameException {

        if (! zkClient.isConnected()) {
            throw new CloudnameException("No connection to storage.");
        }
        synchronized (coordinateDataMonitor) {
            String oldDataSerialized = (null == coordinateData) ? "" : coordinateData.serialize();
            coordinateData = ZkCoordinateData.loadCoordinateData(
                    path, zkClient.getZookeeper(), this).snapshot();
            isSynchronizedWithZookeeper.set(true);
            if (! oldDataSerialized.equals(coordinateData.toString())) {
                client.stateChanged(path);
            }
        }
    }

    private void registerWatcher() throws CloudnameException, InterruptedException {
        try {
            zkClient.getZookeeper().exists(path, this);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        }
    }
}