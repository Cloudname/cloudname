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
import java.util.logging.Logger;

/**
 * Implementation of CloudnameLock. Uses Zookeeper to hold the locks.
 *
 * Algorithm used can be viewed in full in the document called "ZooKeeper Recipes and Solutions" on the
 * official ZooKeeper site.
 *
 * In our words: We create an Ephemeral Sequential node on the scope provided for a coordinate. E.g Scope.SERVICE
 * on the coordinate 1.service.user.cell, with the lockName "MyLock", will result in a created node with
 * the path /cn/cell/user/service/locks/MyLock00000001. This node will exist until the lock is released
 * or ZooKeeper itself deletes it (connectionloss etc resulting in ephemeral node deletion). If the lock
 * node created has the lowest number, the lock is "acquired". The next lowest number in the list will be
 * notified when the lock is removed, if tryLock with a timeout is used and the timeout has not been reached.
 *
 * @author acidmoose
 */
public class ZkCloudnameLock implements CloudnameLock {

    private final ZooKeeper zk;
    private final Coordinate coordinate;
    private final Scope scope;
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
     * @param scope The CloudnameLock.Level where the lock will be in place.
     * @param lockName The name of the lock.
     */
    public ZkCloudnameLock (
        final ZooKeeper zk,
        final Coordinate coordinate,
        final Scope scope,
        final String lockName)
    {
        this.zk = zk;
        this.coordinate = coordinate;
        this.scope = scope;
        this.lockName = lockName;

        final StringBuilder path = new StringBuilder("/" + CloudnameLock.LOCK_FOLDER_NAME + "/" + coordinate.getCell());
        if (scope == Scope.USER || scope == Scope.SERVICE) {
            path.append("/")
                .append(coordinate.getUser());
        }
        if (scope == Scope.SERVICE) {
            path.append("/")
                .append(coordinate.getService());
        }
        path.append("/" + CloudnameLock.LOCK_FOLDER_NAME);
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
                path,
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
                path,
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
            getPathToLockNode(), // "-1" to remove the "/"
            false);

        // Check to see if you hold the lock (find the lock node with the next lower number)
        boolean lockAcquired = true;
        final int createdNumber = getNodeNumber(absoluteLockPath);
        String nodeToWatch = "";
        int nodeNumberToWatch = -1;
        for (final String child : children) {
            if (!child.startsWith(this.lockName)) {
                continue;
            }
            final int childNumber = getNodeNumber(child);
            if (childNumber < createdNumber && childNumber > nodeNumberToWatch) {
                nodeNumberToWatch = childNumber;
                nodeToWatch = child;
                lockAcquired = false;
            }
        }
        if (lockAcquired) {
            // You hold the lock.
            log.log(
                java.util.logging.Level.INFO,
                "Lock " + absoluteLockPath + " aquired.");
            return true;
        } if (!wait) {
            // You do not hold the lock and you do not want to wait. Clean up and return false.
            release();
            return false;
        }

        String lockPathToWatch = getPathToLockNode() + "/" + nodeToWatch;
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

    private String getPathToLockNode() {
        return absoluteLockPath.substring(0, absoluteLockPath.indexOf(lockName) - 1);
    }

    @Override
    public void release() {
        isInUse.set(false);
        try {
            final int anyVersion = -1;
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

    /**
     * Get the trailing number of a sequential ephemeral node. Node path looks like
     * this "/cn/pathToLock/lockname000000001", and this method returns "000000001" in this case.
     * Only use for finding the node number of the current lockname.
     */
    private int getNodeNumber(String node) {
        //TODO (acidmoose): Consider replacing with regex
        return Integer.parseInt(node.substring(node.indexOf(lockName) + lockName.length(), node.length()));
    }

    private String createAndGetLockPath() throws InterruptedException, KeeperException {
        // Create locks if it does not exist
        if (zk.exists(lockPath, false) == null) {
            try {
                Util.mkdir(zk, lockPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
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
