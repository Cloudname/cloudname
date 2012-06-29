package org.cloudname.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.cloudname.CloudnameException;
import org.cloudname.ConfigListener;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.logging.Logger;


/**
 * This class keeps track of config for a coordinate.
 *
 * @author dybdahl
 */
public class TrackedConfig implements Watcher, ZkUserInterface {

    private String configData = null;
    private final ConfigListener configListener;
    
    private static final Logger log = Logger.getLogger(TrackedConfig.class.getName());
    private ZooKeeper zk;
    private final String path;

    private long lastAttemptToCheckIfPresent = 0;
    private boolean upToDate = false;

    /**
     * Constructor, the ZooKeeper instances is retrieved from implementing the ZkUserInterface so the object
     * is not ready to be used before the ZooKeeper instance is received.
     * @param path is the path of the configuration of the coordinate.
     */
    public TrackedConfig(String path, ConfigListener configListener) {
        this.path = path;
        this.configListener = configListener;
    }


    @Override
    public void zooKeeperDown() {
        log.severe("TrackedConfig: Got event ZooKeeper is down.");
        synchronized (this) {
            zk = null;
            upToDate = false;
        }
    }

    @Override
    public void newZooKeeperInstance(ZooKeeper zk) {
        log.severe("TrackedConfig: Got new ZooKeeper!.");
        synchronized (this) {
            this.zk = zk;
        }
        try {
            if (refreshConfigData()) {
                configListener.onConfigEvent(ConfigListener.Event.UPDATED, configData);
            }
        } catch (CloudnameException e) {
            log.severe("TrackedConfig: Got problems reloading config from zookeeper.");
        }
    }

    /**
     * Everything is watch driven, so we don't need to do any periodic checks.
     */
    @Override
    public void timeEvent() {
        synchronized (this) {
            if (upToDate || lastAttemptToCheckIfPresent > System.currentTimeMillis() - 10000) {
                return;
            }
            lastAttemptToCheckIfPresent = System.currentTimeMillis();
        }
        try {
            if (refreshConfigData()) {
                configListener.onConfigEvent(ConfigListener.Event.UPDATED, getConfigData());
            }
        } catch (CloudnameException e) {
            // No worries, we try again later
        }
    }

    /**
     * Returns current config.
     * @return config
     */
    public String getConfigData() {
        synchronized (this) {
            return configData;
        }
    }

    /**
     * Creates a string for debugging etc
     * @return serialized version of the instance data.
     */
    public String toString() {
        return "Config: " + getConfigData();
    }


    /**
     * Handles even from ZooKeeper for this coordinate.
     * @param event
     */
    @Override public void process(WatchedEvent event) {
        log.severe("Got an event from ZooKeeper " + event.toString() + " path: " + path);

        switch (event.getType()) {
            case None:
                switch (event.getState()) {
                    case SyncConnected:
                        break;
                    case Disconnected:
                    case AuthFailed:
                    case Expired:
                    default:
                        upToDate = false;
                        // If we lost connection, we don't attempt to register another watcher as this might
                        // be blocking forever. Parent might try to reconnect.
                        return;
                }
                break;
            case NodeDeleted:
                synchronized (this) {
                    upToDate = false;
                    configData = null;
                }
                configListener.onConfigEvent(ConfigListener.Event.DELETED, "");
                return;
            case NodeDataChanged:
                try {
                    if (refreshConfigData()) {
                        configListener.onConfigEvent(ConfigListener.Event.UPDATED, getConfigData());
                    }
                } catch (CloudnameException e) {
                    log.info("Problems reloading config after change, path; " + path + " " + e.getMessage());
                }
                return;
            case NodeChildrenChanged:
            case NodeCreated:
                break;
        }
        // We are only interested in registering a watcher in a few cases. E.g. if the event is lost connection,
        // registerWatcher does not make sense as it is blocking. In NodeDataChanged above, a watcher
        // is registerred in refreshConfigData().
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
     * Loads the config from ZooKeeper. In case of failure, we keep the old data.
     *
     * @return Returns true if data has changed.
     */
    private boolean refreshConfigData() throws CloudnameException {

        synchronized (this) {
            if (zk == null) {
                throw new CloudnameException("No connection to storage.");
            }
            String oldConfig = configData;
            Stat stat = new Stat();
            try {
                byte[] data;

                data = zk.getData(path, this, stat);
                if (data == null) {
                    configData = "";
                } else {
                    configData = new String(data, Util.CHARSET_NAME);
                }
                upToDate = true;
                return oldConfig == null || ! oldConfig.equals(configData);
            } catch (KeeperException e) {
                throw new CloudnameException(e);
            } catch (UnsupportedEncodingException e) {
                throw new CloudnameException(e);
            } catch (InterruptedException e) {
                throw new CloudnameException(e);
            } catch (IOException e) {
                throw new CloudnameException(e);
            }
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