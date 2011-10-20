package org.cloudname.a3.storage;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;


import org.cloudname.a3.domain.ServiceCoordinate;
import org.cloudname.a3.domain.User;
import org.cloudname.a3.domain.UserDB;

import java.util.Set;
import java.util.HashSet;

import java.util.logging.Logger;
import java.util.logging.Level;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;

/**
 * ZooKeeper based storage for the A3 system.
 *
 * @author borud
 */
public class ZKStorage
    implements UserDBStorage,
               Watcher
{
    private static final Logger log = Logger.getLogger(ZKStorage.class.getName());

    // path related information
    private static final String ACL_PATH_PREFIX = "/acl";
    private static final String USER_DB_NODE = "userdb.json";
    private static final int DEFAULT_SESSION_TIMEOUT = 5000;

    // The paths
    private String datacenterPath;
    private String userPath;
    private String servicePath;
    private String userDBPath;

    private String connectString;
    private ServiceCoordinate serviceCoordinate;
    private int sessionTimeout = DEFAULT_SESSION_TIMEOUT;
    private ZooKeeper zk;

    // Strictly speaking we do not need a set of these
    // but we might as well.
    private Set<UserDBStorage.Watcher> watchers = new HashSet<UserDBStorage.Watcher>();

    // Latches that count down when ZooKeeper is connected
    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    /**
     *
     * @param serviceCoordinate The service coordinate for this service
     * @param connectString The ZooKeeper connect String.  A comma-separated list of host:port pairs.
     */
    public ZKStorage(ServiceCoordinate serviceCoordinate, String connectString) {
        this.serviceCoordinate = serviceCoordinate;
        this.connectString = connectString;

        // Populate the paths for the various parts of the ZooKeeper tree
        datacenterPath = ACL_PATH_PREFIX + "/" + serviceCoordinate.getDatacenter();
        userPath = datacenterPath + "/" + serviceCoordinate.getUser();
        servicePath = userPath + "/" + serviceCoordinate.getServiceName();
        userDBPath = servicePath + "/" + USER_DB_NODE;

    }

    /**
     * Open the storage for IO.  Under the covers this creates a
     * ZooKeeper client and blocks until the connection has been
     * established.
     *
     * @throws A3StorageException if an IOException or
     *   InterruptedException occurs.  Original exception is wrapped.
     */
    @Override
    public void open() {
        try {
            zk = new ZooKeeper(connectString, sessionTimeout, this);
            connectedSignal.await();
            log.info("UserDB opened for " + serviceCoordinate.toString());
        } catch (IOException e) {
            throw new A3StorageException(e);
        } catch (InterruptedException e) {
            throw new A3StorageException(e);
        }
    }

    /**
     * Close the storage and disconnect from ZooKeeper.  Blocks until
     * successfully disconnected or until some error condition is
     * reached.
     *
     * @throws A3StorageException if an error occurs.  Will contain
     *   nested exception.
     */
    @Override
    public void close() {
        if (null == zk) {
            throw new IllegalStateException("Cannot close(): Not connected to ZooKeeper");
        }

        try {
            zk.close();
            log.info("UserDB closed for " + serviceCoordinate.toString());
        } catch (InterruptedException e) {
            throw new A3StorageException(e);
        }

        // Since we will not be needing the watchers anymore we can
        // dispose of the entire set.
        watchers = null;
    }

    @Override
    public void process(WatchedEvent event) {
        log.fine("Got event " + event.toString());
        // If the node changed we must fetch the UserDB and notify watchers.
        if (event.getType() == EventType.NodeDataChanged) {
            processWatchers();
            return;
        }

        // Initial connection to ZooKeeper is completed.
        if (event.getState() == Event.KeeperState.SyncConnected) {
            if (connectedSignal.getCount() == 0) {
                // I am not sure if this can ever occur, but until I
                // know I'll just leave this log message in here.
                log.info("The connectedSignal count was already zero.  Duplicate Event.KeeperState.SyncConnected");
            }
            connectedSignal.countDown();
        }
    }


    /**
     * Ensure that the directory structure for a given service
     * coordinate exists in ZooKeeper.
     *
     */
    private void ensureServicePaths() throws KeeperException, InterruptedException {
        if (null == zk) {
            throw new IllegalStateException("ZKStorage has not been open()'ed");
        }

        // In most cases the service path should exist so we check for that first
        if (zk.exists(serviceCoordinate.getPathPrefix(ACL_PATH_PREFIX, false), false) != null) {
            return;
        }

        // The following code is not very elegant and it probably
        // produces some race conditions if we are to be very picky,
        // but the use-case will mostly be when setting up the service
        // so it isn't really that big of a deal

        // Make sure the ACL_PATH_PREFIX exists
        if (zk.exists(ACL_PATH_PREFIX, false) == null) {
            zk.create(ACL_PATH_PREFIX, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // Make sure Datacenter exists
        if (zk.exists(datacenterPath, false) == null) {
            zk.create(datacenterPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // Make sure User exists
        if (zk.exists(userPath, false) == null) {
            zk.create(userPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // Make sure service exists
        if (zk.exists(servicePath, false) == null) {
            zk.create(servicePath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    @Override
    public void registerUserDBWatcher(UserDBStorage.Watcher watcher) {
        synchronized(watcher) {
            watchers.add(watcher);
        }

        // When a watcher has been register we have to ensure a
        // watcher is registered.  Since the processWatchers() method
        // performs a getUserDB() and getUserDB() sets a new watch we
        // should be covered in terms of always having an active
        // watcher for the userDBPath.
        try {
            zk.exists(userDBPath, true);
        } catch (InterruptedException e) {
            throw new A3StorageException(e);
        } catch (KeeperException e) {
            throw new A3StorageException(e);
        }
    }

    /**
     * This method is called when we know that the user database has
     * been updated so that we can push out a new version to all the
     * watchers.  If no watchers have been added we don't even bother
     * getting the user database and just return.
     */
    private void processWatchers() {
        synchronized(watchers) {
            if (watchers.isEmpty()) {
                // Nothing to do
                return;
            }

            // Fetch the new version of the database
            UserDB db = getUserDB();
            for (UserDBStorage.Watcher w : watchers) {
                w.onUpdate(db);
            }
        }
    }

    /**
     * Create a UserDB and initialize it.
     */
    @Override
    public void createUserDB(UserDB db) {
        try {
            ensureServicePaths();

            // Create the UserDB
            String createdPath = zk.create(userDBPath,
                                           db.toJson().getBytes("UTF-8"),
                                           Ids.OPEN_ACL_UNSAFE,
                                           CreateMode.PERSISTENT);
            // Always starts at version 0 for new nodes
            db.setVersion(0);
            log.info("Created UserDB at " + createdPath);
        } catch(UnsupportedEncodingException e) {
            // This error should never occur
            throw new A3StorageException(e);
        } catch (InterruptedException e) {
            // TODO(borud): should we restart when we get an
            // InterruptedException?
            throw new A3StorageException(e);
        } catch (KeeperException e) {
            throw new A3StorageException(e);
        }
    }

    /**
     * Update a UserDB.  Note that in order to successfully update a
     * UserDB the version number set on the UserDB instance passed to
     * this method must match the version number of the state in
     * ZooKeeper.
     *
     * @param db The UserDB we wish to persist in ZooKeeper
     * @throws A3StorageException which either nests a
     *   InterruptedException or a KeeperException
     */
    @Override
    public void updateUserDB(UserDB db) {
        try {
            Stat stat = zk.setData(userDBPath,
                                   db.toJson().getBytes("UTF-8"),
                                   db.getVersion());
            db.setVersion(stat.getVersion());
            log.info("Updated UserDB at " + userDBPath + ", version = " + db.getVersion());
        } catch(UnsupportedEncodingException e) {
            // This error should never occur
            throw new A3StorageException(e);
        } catch (InterruptedException e) {
            // TODO(borud): should we restart when we get an
            // InterruptedException?
            throw new A3StorageException(e);
        } catch (KeeperException e) {
            throw new A3StorageException(e);
        }

    }

    /**
     * Fetch a UserDB.  Reads the UserDB from the ZooKeeper system and
     * creates an instance which is then returned.
     */
    @Override
    public UserDB getUserDB() {
        try {
            Stat stat = new Stat();
            byte[] data = zk.getData(userDBPath,
                                     true, // watch
                                     stat);
            UserDB db = UserDB.fromJson(new String(data, "UTF-8"));
            db.setVersion(stat.getVersion());
            return db;
        } catch (UnsupportedEncodingException e) {
            // This error should never occur
            throw new A3StorageException(e);
        } catch (InterruptedException e) {
            // TODO(borud): should we restart when we get an
            // InterruptedException?
            throw new A3StorageException(e);
        } catch (KeeperException e) {
            throw new A3StorageException(e);
        }
    }
}