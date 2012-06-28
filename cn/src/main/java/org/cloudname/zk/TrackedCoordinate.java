package org.cloudname.zk;

import org.apache.zookeeper.*;
import org.cloudname.*;

import java.util.logging.Logger;

/**
 * This class keeps track of serviceStatus and endpoints for a coordinate.
 *
 * @author dybdahl
 */
public class TrackedCoordinate implements Watcher, ZkUserInterface {
    /**
     * The client can implement this to get notified on changes.
     */
    public interface ExpressionResolverNotify {
        void stateChanged();
    }

    private ZkCoordinateData.Snapshot coordinateData = null;
    
    private static final Logger log = Logger.getLogger(TrackedCoordinate.class.getName());
    private ZooKeeper zk;
    private final String path;
    private final ExpressionResolverNotify client;
    private boolean needToReloadData = false;

    /**
     * Constructor, the ZooKeeper instances is retrieved from implementing the ZkUserInterface so the object
     * is not ready to be used before the ZooKeeper instance is received.
     * @param path is the path of the status of the coordinate.
     */
    public TrackedCoordinate(ExpressionResolverNotify client, String path) {
        this.path = path;
        this.client = client;
    }


    @Override
    public void zooKeeperDown() {
        log.fine("ClaimedCoordinate: Got event ZooKeeper is down.");
        synchronized (this) {
            zk = null;
            needToReloadData = true;

        }
    }
    
    @Override
    public void newZooKeeperInstance(ZooKeeper zk) {
        log.fine("ClaimedCoordinate: Got new ZooKeeper.");
        synchronized (this) {
            this.zk = zk;
        }
        try {
            if (refreshCoordinateData()) {
                client.stateChanged();
            }
        } catch (CloudnameException e) {
            log.info("Got problems reloading data from zookeeper: " + e.getMessage());
        }
    }

    /**
     * Everything is watch driven, so we don't need to do any periodic checks.
     */
    @Override
    public void timeEvent() {
        synchronized (this) {
            if (needToReloadData == false) {
                return;
            }
        }
        newZooKeeperInstance(zk);
    }


    public ZkCoordinateData.Snapshot getCoordinatedata() {
        return coordinateData;
    }


    /**
     * Creates a string for debugging etc
     * @return serialized version of the instance data.
     */
    public String toString() {
        return coordinateData.toString();
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
                    case SyncConnected:
                        break;
                    case Disconnected:
                    case AuthFailed:
                    case Expired:
                    default:
                        // If we lost connection, we don't attempt to register another watcher as this might
                        // be blocking forever. Parent might try to reconnect.
                        return;
                }
                break;
            case NodeDeleted:
                synchronized (this) {
                    coordinateData = new ZkCoordinateData().snapshot();
                }
                client.stateChanged();
                return;
            case NodeDataChanged:
                try {
                    if (refreshCoordinateData()) {
                        client.stateChanged();
                    }
                } catch (CloudnameException e) {
                    log.info("Problems reloading node after change, path; " + path + " " + e.getMessage());
                }
                return;
            case NodeChildrenChanged:
            case NodeCreated:
                break;
        }
        try {
            registerWatcher();
        } catch (CloudnameException e) {
            log.info("Got cloudname exception: " + e.getMessage());
            return;
        } catch (InterruptedException e) {
            log.info("Got interrupted exception: " + e.getMessage());
            return;
        }
    }


    /**
     * Loads the coordinate from ZooKeeper. In case of failure, we keep the old data.
     *
     * @return Returns true if data has changed.
     */
    private boolean refreshCoordinateData() throws CloudnameException {

        synchronized (this) {
            if (zk == null) {
                needToReloadData = true;
                throw new CloudnameException("No connection to storage.");
            }
            String oldDataSerialized = new String("");
            if (null != coordinateData) {
                oldDataSerialized = coordinateData.serialize();
            }
            coordinateData = ZkCoordinateData.loadCoordinateData(path, zk, this).snapshot();
            needToReloadData = false;
            return (! oldDataSerialized.equals(coordinateData.toString()));
        }
    }

    private void registerWatcher() throws CloudnameException, InterruptedException {
        try {
            synchronized (this) {
                zk.exists(path, this);
            }
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        }
    }
}