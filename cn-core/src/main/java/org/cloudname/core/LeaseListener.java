package org.cloudname.core;

/**
 * Lease notifications to clients.
 *
 * @author stalehd@gmail.com
 */
public interface LeaseListener {
    /**
     * A new lease is created.
     *
     * @param path The full path of the lease
     * @param data The data stored on the lease
     */
    void leaseCreated(final CloudnamePath path, final String data);

    /**
     * A lease is removed. The lease might not exist anymore at this point in time.
     *
     * @param path The path of the lease.
     */
    void leaseRemoved(final CloudnamePath path);

    /**
     * Lease data have changed in one of the leases the client is listening on.
     *
     * @param path Full path to the lease that have changed
     * @param data The new data element stored in the lease
     */
    void dataChanged(final CloudnamePath path, final String data);
}
