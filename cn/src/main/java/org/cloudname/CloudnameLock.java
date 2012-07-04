package org.cloudname;

import java.util.concurrent.TimeUnit;

/**
 * A lock service for use with cloudname.
 *
 * A lock can be placed on any given level of a Coordinate. E.g a lock on any instance of a service (in the
 * example below) uses CloudnameLock.Level.SERVICE.
 *
 * Usage example:
 *
 * CloudnameLock lock = base.getServiceHandle().getCloudnameLock(CloudnameLock.Level.SERVICE, "serviceLock");
 *
 * lock.addListener(new CloudnameLockListener() {
 *     @Override
 *     public void lost() {
 *         // Connection or lock lost. Interrupt work if necessary.
 *     }
 *  });
 *
 *  if (lock.tryLock(TIMEOUT_MS)) {
 *      doWork();
 *      lock.release();
 *  } else {
 *      System.out.println("Unable to perform work within alotted time.");
 *  }
 *
 * @author acidmoose
 */
public interface CloudnameLock {

    /**
     * The levels where a lock can be placed based on the coordinate of the lock owner.
     */
    public enum Level {
        CELL,
        USER,
        SERVICE
    }

    /**
     * Add a listener on the lock to get notified if a lock is lost.
     * @param cloudnameLockListener
     */
    void addListener(CloudnameLockListener cloudnameLockListener);

    /**
     * Attempt to acquire the lock.
     * @return true if success, false if a lock could not be obtained.
     */
    public boolean tryLock();

    /**
     * Attempt to acquire the lock. Will wait for a lock to open.
     * @param timeoutMs Time in milliseconds to wait for a lock.
     * @return true if success, false if a lock could not be obtained before the timeout.
     */
    public boolean tryLock(int timeoutMs);

    /**
     * Release the lock. After released the CloudnameLock object can be reused to lock again.
     */
    public void release();
}
