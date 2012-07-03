package org.cloudname;

/**
 * Listener interface for use when a CloudnameLock can be lost.
 *
 * @author acidmoose
 */
public interface CloudnameLockListener {
    /**
     * Called if you have lost the lock, or if it is no longer certain if you have the lock.
     */
    public void lost();
}
