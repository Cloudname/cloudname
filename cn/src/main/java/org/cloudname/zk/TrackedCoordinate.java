package org.cloudname.zk;

import org.apache.zookeeper.*;
import org.cloudname.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
        void stateChanged();
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
        this.path = path;
        this.client = client;
        this.zkClient = zkClient;
    }


    @Override
    public void connectionUp() {
    }

    @Override
    public void connectionDown() {
        isSynchronizedWithZookeeper.set(false);
    }

    public void refreshAsync() {
        isSynchronizedWithZookeeper.set(false);
    }

    public void start() {
        zkClient.registerListener(this);
        final  long periodicDelayMs = 2000;
        scheduler.scheduleWithFixedDelay(new ResolveProblems(), 1 /* initial delay ms */,
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
     * Everything is watch driven, so we don't need to do any periodic checks.
     */
    class ResolveProblems implements Runnable {
        @Override
        public void run() {
            if (! isSynchronizedWithZookeeper.getAndSet(true)) {  return;  }
            try {
                refreshCoordinateData();

            } catch (CloudnameException e) {
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
                client.stateChanged();
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
            LOG.info("Got cloudname exception: " + e.getMessage());
        } catch (InterruptedException e) {
            LOG.info("Got interrupted exception: " + e.getMessage());
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
        String oldDataSerialized = "";
        synchronized (coordinateDataMonitor) {
            if (null != coordinateData) {
                oldDataSerialized = coordinateData.serialize();
            }
            coordinateData = ZkCoordinateData.loadCoordinateData(
                    path, zkClient.getZookeeper(), this).snapshot();
            isSynchronizedWithZookeeper.set(true);
            if (! oldDataSerialized.equals(coordinateData.toString())) {
                client.stateChanged();
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