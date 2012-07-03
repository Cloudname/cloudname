package org.cloudname.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.cloudname.CloudnameException;
import org.cloudname.CloudnameLock;
import org.cloudname.CloudnameLockListener;
import org.cloudname.Coordinate;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of CloudnameLock. Uses Zookeeper to hold the locks.
 *
 * @author acidmoose
 */
public class ZkCloudnameLock implements CloudnameLock {

    private final ZooKeeper zk;
    private final Coordinate coordinate;
    private final Level level;
    private final String lockName;

    // This will be false if a lock is obtained, so that you can not lock twice with the same object.
    private boolean isInUse = false;
    private String lockPath;

    private static final Logger log = Logger.getLogger(ZkCloudnameLock.class.getName());

    /**
     * Prepare a CloudnameLock object.
     * @param zk ZooKeeper instance to use.
     * @param coordinate The coordinate responsible for the lock.
     * @param level The CloudnameLock.Level where the lock will be in place.
     * @param lockName The name of the lock.
     */
    public ZkCloudnameLock (
        final ZooKeeper zk,
        final Coordinate coordinate,
        final Level level,
        final String lockName
    ) {
        this.zk = zk;
        this.coordinate = coordinate;
        this.level = level;
        this.lockName = lockName;
    }

    @Override
    public void addListener(CloudnameLockListener cloudnameLockListener) {
        // TODO(acidmoose): Implement
    }

    @Override
    public boolean lock() {
        // This lock object is already in use
        if (isInUse) {
            return false;
        }
        isInUse = true;

        final String path;
        try {
            path = getLockPath();
        } catch (InterruptedException e) {
            log.log(
                java.util.logging.Level.INFO,
                "InterruptedException while trying to get lock path " + lockPath,
                e);
            return false;
        } catch (KeeperException e) {
            log.log(
                java.util.logging.Level.INFO,
                "KeeperException while trying to get lock path " + lockPath,
                e);
            return false;
        }

        try {
            // Create lock.
            lockPath = zk.create(
                path.toString(),
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
            log.log(
                java.util.logging.Level.INFO,
                "Created lock node with path: " + lockPath);

            long noTimeout = 0;
            boolean noWait = false;
            return attemptToLock(noTimeout, noWait);

        } catch (InterruptedException e) {
            log.log(
                java.util.logging.Level.INFO,
                "InterruptedException while trying to create lock " + lockPath,
                e);
            return false;
        } catch (KeeperException e) {
            log.log(
                java.util.logging.Level.INFO,
                "KeeperException while trying to create lock " + lockPath,
                e);
            return false;
        }
    }

    @Override
    public boolean waitForLockMs(int timeout) {
        // This lock object is allready in use
        if (isInUse) {
            return false;
        }
        isInUse = true;

        final String path;
        try {
            path = getLockPath();
        } catch (InterruptedException e) {
            log.log(
                java.util.logging.Level.INFO,
                "InterruptedException while trying to get lock path " + lockPath,
                e);
            return false;
        } catch (KeeperException e) {
            log.log(
                java.util.logging.Level.INFO,
                "KeeperException while trying to get lock path " + lockPath,
                e);
            return false;
        }

        try {
            // Create lock.
            lockPath = zk.create(
                path.toString(),
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
            log.log(
                java.util.logging.Level.INFO,
                "Created lock node with path: " + lockPath);

            boolean wait = true;
            return attemptToLock(timeout, wait);

        } catch (InterruptedException e) {
            log.log(
                java.util.logging.Level.INFO,
                "InterruptedException while trying to create lock " + lockPath,
                e);
            return false;
        } catch (KeeperException e) {
            log.log(
                java.util.logging.Level.INFO,
                "KeeperException while trying to create lock " + lockPath,
                e);
            return false;
        }
    }

    private boolean attemptToLock(long timeout, boolean wait) throws InterruptedException, KeeperException {
        log.log(
            java.util.logging.Level.INFO,
            "Attempting to lock " + lockPath + ".");
        final long time = System.currentTimeMillis();
        final CountDownLatch timeoutLatch = new CountDownLatch(1);
        List<String> children = zk.getChildren(lockPath.substring(0, lockPath.indexOf(lockName) - 1), false);

        // Check to see if you hold the lock (find the lock node with the next lower number)
        boolean lockAquired = true;
        final int createdNumber = getNodeNumber(lockPath);
        String nodeToWatch = "";
        int nodeNumberToWatch = -1;
        for (final String child : children) {
            final int childNumber = getNodeNumber(child);
            if (childNumber < createdNumber && childNumber > nodeNumberToWatch) {
                nodeNumberToWatch = childNumber;
                nodeToWatch = child;
                lockAquired = false;
            }
        }
        if (lockAquired) {
            // You hold the lock.
            log.log(
                java.util.logging.Level.INFO,
                "Lock " + lockPath + " aquired.");
            return true;
        } else if (!wait) {
            // You do not hold the lock and you do not want to wait. Clean up and return false.
            release();
            return false;
        }

        String lockPathToWatch = lockPath.substring(0, lockPath.indexOf(lockName)) + nodeToWatch;
        log.log(
            java.util.logging.Level.INFO,
            lockPath + " is waiting for " + lockPathToWatch + ".");
        Stat stat = zk.exists(lockPathToWatch, new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType().equals(Event.EventType.NodeDeleted)) {
                    log.log(
                        java.util.logging.Level.INFO,
                        "Lock node " + lockPath + " received event of lock removed event..");
                    timeoutLatch.countDown();
                } else {
                    System.out.println("Got another event!!!");
                }
            }
        });
        if (stat == null) {
            log.log(
                java.util.logging.Level.INFO,
                "Another service has released a lock.");
            timeoutLatch.countDown();
        }

        log.log(
            java.util.logging.Level.INFO,
            "Waiting for lock " + lockPath + ". Remaining time: " + timeout + " ms.");
        if ( ! timeoutLatch.await(timeout, TimeUnit.MILLISECONDS)) {
            // Timed out while waiting for lock
            log.log(java.util.logging.Level.INFO, "Timed out while waiting for lock.");
            int anyVersion = -1;
            zk.delete(lockPath, anyVersion);
            isInUse = false;
            return false;
        }

        // The watched node has been deleted. Attempt to lock again with the remaining time left.
        return attemptToLock(timeout - (System.currentTimeMillis() - time), wait);
    }

    @Override
    public void release() {
        isInUse = false;
        int anyVersion = -1;
        try {
            zk.delete(lockPath, anyVersion);
            log.log(
                java.util.logging.Level.INFO,
                "Released lock " + lockPath);
        } catch (InterruptedException e) {
            log.log(
                java.util.logging.Level.INFO,
                "InterruptedException while trying to release lock " + lockPath,
                e);
        } catch (KeeperException e) {
            log.log(
                java.util.logging.Level.INFO,
                "KeeperException while trying to release lock " + lockPath,
                e);
        }
    }

    private int getNodeNumber(String node) {
        return Integer.parseInt(node.substring(node.indexOf(lockName) + lockName.length(), node.length()));
    }

    private String getLockPath() throws InterruptedException, KeeperException {
        final StringBuffer path = new StringBuffer("/cn/" + coordinate.getCell());
        if (level == Level.USER || level == Level.SERVICE) {
            path.append("/")
                .append(coordinate.getUser());
        }
        if (level == Level.SERVICE) {
            path.append("/")
                .append(coordinate.getService());
        }
        path.append("/locks");

        // Create locks if it does not exist
        if (zk.exists(path.toString(), false) == null) {
            try {
                Util.mkdir(zk, path.toString(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
            } catch (CloudnameException e) {
                log.log(
                    java.util.logging.Level.INFO,
                    "CloudnameException while trying to get lock path " + lockPath,
                    e);
            }
        }

        return path.append("/").append(lockName).toString();
    }
}
