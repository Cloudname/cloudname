package org.cloudname;

/**
 * Listener interface for use when a CloudnameLock can be lost.
 *
 * @author acidmoose
 */
public interface CloudnameLockListener {
    /**
     * Called if you have lost the lock, or if it is no longer certain if you have the lock. A lock
     * can be lost after it has been acquired. It is considered as a system error that might happen.
     * It has to be refetched/relocked. It might mean that somebody else grabbed it.
     */
    public void lost();
}
