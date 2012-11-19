package org.cloudname.zk;

import org.apache.zookeeper.data.Stat;
import org.cloudname.*;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException;

import java.io.UnsupportedEncodingException;
import java.util.List;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.CountDownLatch;

import java.io.IOException;


/**
 * An implementation of Cloudname using ZooKeeper.
 *
 * This implementation assumes that the path prefix defined by
 * CN_PATH_PREFIX is only used by Cloudname.  The structure and
 * semantics of things under this prefix are defined by this library
 * and will be subject to change.
 *
 *
 * @author borud
 * @author dybdahl
 * @author storsveen
 */
public final class ZkCloudname implements Cloudname, Watcher, Runnable {

    private static final int SESSION_TIMEOUT = 5000;

    private static final Logger log = Logger.getLogger(ZkCloudname.class.getName());

    private ZkObjectHandler zkObjectHandler = null;

    private final String connectString;

    // Latches that count down when ZooKeeper is connected
    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    private ZkResolver resolver = null;

    private int connectingCounter = 0;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    private ZkCloudname(final Builder builder) {
        connectString = builder.getConnectString();

    }

    /**
     * Checks state of zookeeper connection and try to keep it running.
     */
    @Override
    public void run() {
        final ZooKeeper.States state = zkObjectHandler.getClient().getZookeeper().getState();

        if (state == ZooKeeper.States.CONNECTED) {
            zkObjectHandler.connectionUp();
            connectingCounter = 0;
            return;
        }

        zkObjectHandler.connectionDown();

        if (state == ZooKeeper.States.CONNECTING) {
            connectingCounter++;
            if (connectingCounter > 10) {
                log.fine("Long time in connecting, forcing a close of zookeeper client.");
                zkObjectHandler.close();
                connectingCounter = 0;
            }
            return;
        }

        if (state == ZooKeeper.States.CLOSED) {
            log.fine("Retrying connection to ZooKeeper.");
            try {
                zkObjectHandler.setZooKeeper(
                        new ZooKeeper(connectString, SESSION_TIMEOUT, this));
            } catch (IOException e) {
                log.log(Level.SEVERE, "RetryConnection failed for some reason:"
                        + e.getMessage(), e);
            }
            return;
        }

        log.severe("Unknown state " + state + " closing....");
        zkObjectHandler.close();
    }


    /**
     * Connect to ZooKeeper instance with time-out value.
     * @param waitTime time-out value for establishing connection.
     * @param waitUnit time unit for time-out when establishing connection.
     * @throws CloudnameException if connection can not be established
     * @return
     */
    public ZkCloudname connectWithTimeout(long waitTime, TimeUnit waitUnit)
            throws CloudnameException {

        boolean connected = false;
        try {

            zkObjectHandler = new ZkObjectHandler(
                    new ZooKeeper(connectString, SESSION_TIMEOUT, this));
                          
            if (! connectedSignal.await(waitTime, waitUnit)) {
                throw new CloudnameException("Connecting to ZooKeeper timed out.");
            }
            log.fine("Connected to ZooKeeper " + connectString);
            connected = true;
        } catch (IOException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } finally {
            if (!connected) {
                zkObjectHandler.close();
            }
        }
        resolver =  new ZkResolver.Builder().addStrategy(new StrategyAll())
                .addStrategy(new StrategyAny()).build(zkObjectHandler.getClient());
        scheduler.scheduleWithFixedDelay(this, 1 /* initial delay ms */,
                1000 /* check state every second  */, TimeUnit.MILLISECONDS);
        return this;
    }

    /**
     * Connect to ZooKeeper instance with long time-out, however, it might fail fast.
     * @return connected ZkCloudname object
     * @throws CloudnameException if connection can not be established.
     */
    public ZkCloudname connect() throws CloudnameException {
        // We wait up to 100 years.
        return connectWithTimeout(365 * 100, TimeUnit.DAYS);
    }



    @Override
    public void process(WatchedEvent event) {
        log.fine("Got event in ZkCloudname: " + event.toString());
        if (event.getState() == Event.KeeperState.Disconnected
                || event.getState() == Event.KeeperState.Expired) {
            zkObjectHandler.connectionDown();
        }
        
        // Initial connection to ZooKeeper is completed.
        if (event.getState() == Event.KeeperState.SyncConnected) {
            zkObjectHandler.connectionUp();
            // The first connection set up is blocking, this will unblock the connection.
            connectedSignal.countDown();
        }
    }

    /**
     * Create a given coordinate in the ZooKeeper node tree.
     *
     * Just blindly creates the entire path.  Elements of the path may
     * exist already, but it seems wasteful to
     * @throws CoordinateExistsException if coordinate already exists-
     * @throws CloudnameException if problems with zookeeper connection.
     */
    @Override
    public void createCoordinate(final Coordinate coordinate)
            throws CloudnameException, CoordinateExistsException {
        // Create the root path for the coordinate.  We do this
        // blindly, meaning that if the path already exists, then
        // that's ok -- so a more correct name for this method would
        // be ensureCoordinate(), but that might confuse developers.
        String root = ZkCoordinatePath.getCoordinateRoot(coordinate);
        final ZooKeeper zk = zkObjectHandler.getClient().getZookeeper();
        try {
            if (Util.exist(zk, root)) {
                throw new CoordinateExistsException("Coordinate already created:" +root);
            }
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }

        try {
            Util.mkdir(zk, root, Ids.OPEN_ACL_UNSAFE);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }

        // Create the nodes that represent subdirectories.
        String configPath = ZkCoordinatePath.getConfigPath(coordinate, null);
        try {
            log.fine("Creating config node " + configPath);
            zk.create(configPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
    }

    /**
     * Deletes a coordinate in the persistent service store. This includes deletion
     * of config. It will fail if the coordinate is claimed.
     * @param coordinate the coordinate we wish to destroy.
     */
    @Override
    public void destroyCoordinate(final Coordinate coordinate)
            throws CoordinateDeletionException, CoordinateMissingException, CloudnameException {
        String statusPath = ZkCoordinatePath.getStatusPath(coordinate);
        String configPath = ZkCoordinatePath.getConfigPath(coordinate, null);
        String rootPath = ZkCoordinatePath.getCoordinateRoot(coordinate);
        final ZooKeeper zk = zkObjectHandler.getClient().getZookeeper();
        try {
            if (! Util.exist(zk, rootPath)) {
                throw new CoordinateMissingException("Coordinate not found: " + rootPath);
            }
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }


        // Do this early to raise the error before anything is deleted. However, there might be a
        // race condition if someone claims while we delete configPath and instance (root) node.
        try {
            if (Util.exist(zk, configPath) && Util.hasChildren(zk, configPath)) {
                throw new CoordinateDeletionException("Coordinate has config node.");
            }
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }

        try {
            if (Util.exist(zk, statusPath)) {
                throw new CoordinateDeletionException("Coordinate is claimed.");
            }
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }

        // Delete config, the instance node, and continue with as much as possible.
        // We might have a raise condition if someone is creating a coordinate with a shared path
        // in parallel. We want to keep 3 levels of nodes (/cn/%CELL%/%USER%).
        int deletedNodes = 0;
        try {
            deletedNodes = Util.deletePathKeepRootLevels(zk, configPath, 3);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
        if (deletedNodes == 0) {
            throw new CoordinateDeletionException("Failed deleting config node, nothing deleted..");
        }
        if (deletedNodes == 1) {
            throw new CoordinateDeletionException("Failed deleting instance node.");
        }
    }

    /**
     * Claim a coordinate.
     *
     * In this implementation a coordinate is claimed by creating an
     * ephemeral with the name defined in CN_STATUS_NAME.  If the node
     * already exists the coordinate has already been claimed.
     */
    @Override
    public ServiceHandle claim(final Coordinate coordinate) {
        String statusPath = ZkCoordinatePath.getStatusPath(coordinate);
        log.fine("Claiming " + coordinate.asString() + " (" + statusPath + ")");

        ClaimedCoordinate statusAndEndpoints = new ClaimedCoordinate(
                coordinate, zkObjectHandler.getClient());

        // If we have come thus far we have succeeded in creating the
        // CN_STATUS_NAME node within the service coordinate directory
        // in ZooKeeper and we can give the client a ServiceHandle.
        ZkServiceHandle handle = new ZkServiceHandle(
                statusAndEndpoints, coordinate, zkObjectHandler.getClient());
        statusAndEndpoints.start();
        return handle;
    }
    
    @Override
    public Resolver getResolver() {

        return resolver;
    }

    @Override
    public ServiceStatus getStatus(Coordinate coordinate) throws CloudnameException {
        String statusPath = ZkCoordinatePath.getStatusPath(coordinate);
        ZkCoordinateData zkCoordinateData = ZkCoordinateData.loadCoordinateData(
                statusPath, zkObjectHandler.getClient().getZookeeper(), null);
        return zkCoordinateData.snapshot().getServiceStatus();
    }

    @Override
    public void setConfig(
            final Coordinate coordinate, final String newConfig, final String oldConfig)
            throws CoordinateMissingException, CloudnameException {
        String configPath = ZkCoordinatePath.getConfigPath(coordinate, null);
        int version = -1;
        final ZooKeeper zk = zkObjectHandler.getClient().getZookeeper();
        if (oldConfig != null) {
            Stat stat = new Stat();
            byte [] data = null;
            try {
                data = zk.getData(configPath, false, stat);
            } catch (KeeperException e) {
                throw new CloudnameException(e);
            } catch (InterruptedException e) {
                throw new CloudnameException(e);
            }
            try {
                String stringData = new String(data, Util.CHARSET_NAME);
                if (! stringData.equals(oldConfig)) {
                    throw new CloudnameException("Data did not match old config. Actual old "
                            + stringData + " specified old " + oldConfig);
                }
            } catch (UnsupportedEncodingException e) {
                throw new CloudnameException(e);
            }
            version = stat.getVersion();
        }
        try {
            zk.setData(configPath, newConfig.getBytes(Util.CHARSET_NAME), version);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (UnsupportedEncodingException e) {
            throw new CloudnameException(e);
        }
    }


    @Override
    public String getConfig(final Coordinate coordinate)
            throws CoordinateMissingException, CloudnameException {
        String configPath = ZkCoordinatePath.getConfigPath(coordinate, null);
        Stat stat = new Stat();
        try {
            byte[] data = zkObjectHandler.getClient().getZookeeper().getData(
                    configPath, false, stat);
            if (data == null) {
                return null;
            }
            return new String(data, Util.CHARSET_NAME);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (UnsupportedEncodingException e) {
            throw new CloudnameException(e);
        }
    }
    
    /**
     * Close the connection to ZooKeeper.
     */
    public void close() throws InterruptedException {
        zkObjectHandler.close();
        log.fine("ZooKeeper session closed for " + connectString);
        scheduler.shutdown();
    }

    /**
     * List the sub-nodes in ZooKeeper owned by Cloudname.
     * @param nodeList
     */
    public void listRecursively(List<String> nodeList)
            throws CloudnameException, InterruptedException {
        Util.listRecursively(zkObjectHandler.getClient().getZookeeper(),
                ZkCoordinatePath.getCloudnameRoot(), nodeList);
    }

    /**
     *  This class builds parameters for ZkCloudname.
     */
    public static class Builder {
        private String connectString;

        public Builder setConnectString(String connectString) {
            this.connectString = connectString;
            return this;
        }

        // TODO(borud, dybdahl): Make this smarter, some ideas:
        //                       Connect to one node and read from a magic path
        //                       how many zookeepers that are running and build
        //                       the path based on this information.
        public Builder setDefaultConnectString() {
            this.connectString = "z1:2181,z2:2181,z3:2181";
            return this;
        }

        public String getConnectString() {
            return connectString;
        }

        public ZkCloudname build() {
            if (connectString.isEmpty()) {
                throw new RuntimeException(
                        "You need to specify connection string before you can build.");
            }
            return new ZkCloudname(this);
        }
    }
}
