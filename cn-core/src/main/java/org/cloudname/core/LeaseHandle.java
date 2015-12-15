package org.cloudname.core;

/**
 * Handle returned by the backend when a temporary lease is created.
 *
 * @author stalehd@gmail.com
 */
public interface LeaseHandle extends AutoCloseable {
    /**
     * Write data to the lease.
     *
     * @param data  data to write. Cannot be null.
     * @return true if data is written
     */
    boolean writeLeaseData(final String data);

    /**
     * The full path of the lease.
     */
    CloudnamePath getLeasePath();
}
