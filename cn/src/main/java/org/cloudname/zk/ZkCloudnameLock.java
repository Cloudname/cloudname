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
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final AtomicBoolean isInUse = new AtomicBoolean(false);
    private String absoluteLockPath;
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
        lockPath = path.toString();
    }

    @Override
    public void addListener(CloudnameLockListener cloudnameLockListener) {
        // TODO(acidmoose): Implement
    }

    @Override
    public boolean tryLock() {
        // This lock object is already in use
        if (!isInUse.compareAndSet(false, true)) {
            return false;
        }

        final String path;
        try {
            path = createAndGetLockPath();
        } catch (InterruptedException e) {
            log.log(
                java.util.logging.Level.WARNING,
                "InterruptedException while trying to get lock path " + absoluteLockPath,
                e);
            return false;
        } catch (KeeperException e) {
            log.log(
                java.util.logging.Level.WARNING,
                "KeeperException while trying to get lock path " + absoluteLockPath,
                e);
            return false;
        }

        try {
            // Create lock.
            absoluteLockPath = zk.create(
                path.toString(),
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
            log.log(
                java.util.logging.Level.INFO,
                "Created lock node with path: " + absoluteLockPath);

            final long noTimeout = 0;
            final boolean noWait = false;
            return attemptToLock(noTimeout, noWait);

        } catch (InterruptedException e) {
            log.log(
                java.util.logging.Level.INFO,
                "InterruptedException while trying to create lock " + absoluteLockPath,
                e);
            return false;
        } catch (KeeperException e) {
            log.log(
                java.util.logging.Level.INFO,
                "KeeperException while trying to create lock " + absoluteLockPath,
                e);
            return false;
        }
    }

    @Override
    public boolean tryLock(int timeoutMs) {
        // This lock object is allready in use
        if (!isInUse.compareAndSet(false, true)) {
            return false;
        }

        final String path;
        try {
            path = createAndGetLockPath();
        } catch (InterruptedException e) {
            log.log(
                java.util.logging.Level.WARNING,
                "InterruptedException while trying to get lock path " + absoluteLockPath,
                e);
            return false;
        } catch (KeeperException e) {
            log.log(
                java.util.logging.Level.WARNING,
                "KeeperException while trying to get lock path " + absoluteLockPath,
                e);
            return false;
        }

        try {
            // Create lock.
            absoluteLockPath = zk.create(
                path.toString(),
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
            log.log(
                java.util.logging.Level.INFO,
                "Created lock node with path: " + absoluteLockPath);

            final boolean wait = true;
            return attemptToLock(timeoutMs, wait);

        } catch (InterruptedException e) {
            log.log(
                java.util.logging.Level.INFO,
                "InterruptedException while trying to create lock " + absoluteLockPath,
                e);
            return false;
        } catch (KeeperException e) {
            log.log(
                java.util.logging.Level.INFO,
                "KeeperException while trying to create lock " + absoluteLockPath,
                e);
            return false;
        }
    }

    private boolean attemptToLock(long timeoutMs, boolean wait) throws InterruptedException, KeeperException {
        log.log(
            java.util.logging.Level.INFO,
            "Attempting to lock " + absoluteLockPath + ".");
        final long time = System.currentTimeMillis();
        final CountDownLatch timeoutLatch = new CountDownLatch(1);
        List<String> children = zk.getChildren(
            absoluteLockPath.substring(0, absoluteLockPath.indexOf(lockName) - 1), // "-1" to remove the "/"
            false);

        // Check to see if you hold the lock (find the lock node with the next lower number)
        boolean lockAquired = true;
        final int createdNumber = getNodeNumber(absoluteLockPath);
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
                "Lock " + absoluteLockPath + " aquired.");
            return true;
        } else if (!wait) {
            // You do not hold the lock and you do not want to wait. Clean up and return false.
            release();
            return false;
        }

        String lockPathToWatch = absoluteLockPath.substring(0, absoluteLockPath.indexOf(lockName)) + nodeToWatch;
        log.log(
            java.util.logging.Level.INFO,
            absoluteLockPath + " is waiting for " + lockPathToWatch + ".");
        Stat stat = zk.exists(lockPathToWatch, new Watcher() {

            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType().equals(Event.EventType.NodeDeleted)) {
                    log.log(
                        java.util.logging.Level.INFO,
                        "Lock node " + absoluteLockPath + " received event of lock removed event..");
                    timeoutLatch.countDown();
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
            "Waiting for lock " + absoluteLockPath + ". Remaining time: " + timeoutMs + " ms.");
        if ( ! timeoutLatch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
            // Timed out while waiting for lock
            log.log(java.util.logging.Level.INFO, "Timed out while waiting for lock.");
            int anyVersion = -1;
            zk.delete(absoluteLockPath, anyVersion);
            isInUse.set(false);
            return false;
        }

        // The watched node has been deleted. Attempt to lock again with the remaining time left.
        return attemptToLock(timeoutMs - (System.currentTimeMillis() - time), wait);
    }

    @Override
    public void release() {
        isInUse.set(false);
        int anyVersion = -1;
        try {
            zk.delete(absoluteLockPath, anyVersion);
            log.log(
                java.util.logging.Level.INFO,
                "Released lock " + absoluteLockPath);
        } catch (InterruptedException e) {
            log.log(
                java.util.logging.Level.INFO,
                "InterruptedException while trying to release lock " + absoluteLockPath,
                e);
        } catch (KeeperException e) {
            log.log(
                java.util.logging.Level.INFO,
                "KeeperException while trying to release lock " + absoluteLockPath,
                e);
        }
    }

    private int getNodeNumber(String node) {
        return Integer.parseInt(node.substring(node.indexOf(lockName) + lockName.length(), node.length()));
    }

    private String createAndGetLockPath() throws InterruptedException, KeeperException {
        // Create locks if it does not exist
        if (zk.exists(lockPath.toString(), false) == null) {
            try {
                Util.mkdir(zk, lockPath.toString(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
            } catch (CloudnameException e) {
                log.log(
                    java.util.logging.Level.INFO,
                    "CloudnameException while trying to get lock path " + absoluteLockPath,
                    e);
            }
        }

        return lockPath + "/" + lockName;
    }
}
