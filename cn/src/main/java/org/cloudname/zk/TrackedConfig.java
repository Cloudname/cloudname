package org.cloudname.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.cloudname.CloudnameException;
import org.cloudname.ConfigListener;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;


/**
 * This class keeps track of config for a coordinate.
 *
 * @author dybdahl
 */
public class TrackedConfig implements Watcher, ZkObjectHandler.ConnectionStateChanged {

    private String configData = null;
    private final ConfigListener configListener;

    private static final Logger log = Logger.getLogger(TrackedConfig.class.getName());

    private final String path;

    private AtomicBoolean isSynchronizedWithZookeeper = new AtomicBoolean(false);

    private final ZkObjectHandler.Client zkClient;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();
    /**
     * Constructor, the ZooKeeper instances is retrieved from ZkObjectHandler.Client,
     * so we won't get it until the client reports we have a Zk Instance in the handler.
     * @param path is the path of the configuration of the coordinate.
     */
    public TrackedConfig(String path, ConfigListener configListener, ZkObjectHandler.Client zkClient) {
        this.path = path;
        this.configListener = configListener;
        this.zkClient = zkClient;
    }


    @Override
    public void connectionUp() {
    }

    @Override
    public void connectionDown() {
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




    /**
     * Everything is watch driven, so we don't need to do any periodic checks.
     */
    class ResolveProblems implements Runnable {
        @Override
        public void run() {

            synchronized (this) {
                if (isSynchronizedWithZookeeper.get())
                    return;
            }
            try {
                if (refreshConfigData()) {
                    configListener.onConfigEvent(ConfigListener.Event.UPDATED, getConfigData());
                }
            } catch (CloudnameException e) {
                // No worries, we try again later
            }
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
     * Handles event from ZooKeeper for this coordinate.
     * @param event Event to handle
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
                        isSynchronizedWithZookeeper.set(false);
                        // If we lost connection, we don't attempt to register another watcher as this might
                        // be blocking forever. Parent might try to reconnect.
                        return;
                }
                break;
            case NodeDeleted:
                synchronized (this) {
                    isSynchronizedWithZookeeper.set(false);
                    configData = null;
                }
                configListener.onConfigEvent(ConfigListener.Event.DELETED, "");
                return;
            case NodeDataChanged:
                isSynchronizedWithZookeeper.set(false);
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
            if (! zkClient.isConnected()) {
                throw new CloudnameException("No connection to storage.");
            }
            String oldConfig = configData;
            Stat stat = new Stat();
            try {
                byte[] data;

                data = zkClient.getZookeeper().getData(path, this, stat);
                if (data == null) {
                    configData = "";
                } else {
                    configData = new String(data, Util.CHARSET_NAME);
                }
                isSynchronizedWithZookeeper.set(true);
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
            zkClient.getZookeeper().exists(path, this);

        } catch (KeeperException e) {
            throw new CloudnameException(e);
        }
    }

}