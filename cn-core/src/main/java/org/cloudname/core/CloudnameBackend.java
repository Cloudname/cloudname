package org.cloudname.core;

/**
 * Cloudname supports different backends which implements this interface. This interface isn't used
 * by clients directly but the clients will interface with libraries built on top of this interface.
 *
 * <p>There are two kinds of leases - permanent and temporary. The permanent leases persist in the
 * backend and aren't removed when clients disconnect, even if *all* clients disconnect.
 * The temporary leases are removed by the backend when the client closes. Note that clients might
 * not be well-behaved and may terminate without calling close(). The backend should remove
 * these leases automatically.
 *
 * <p>Clients listen on both kinds of leases and get notifications through listeners whenever
 * something is changed. Notifications to the clients are sent in the same order they are received.
 *
 * <p>Each lease has a data string attached to the lease and clients may update this freely.
 *
 * @author stalehd@gmail.com
 */
public interface CloudnameBackend extends AutoCloseable {
    /**
     * Create a temporary lease. The temporary lease is limited by the client's connection and will
     * be available for as long as the client is connected to the backend. Once the client
     * disconnects (either through the LeaseHandle instance that is returned or just vanishing
     * from the face of the earth) the lease is removed by the backend. The backend should support
     * an unlimited number of leases (FSVO "unlimited").
     *
     * @param path Path to temporary lease. This value cannot be null. The path supplied by the
     *     client is just the stem of the full lease, i.e. if a client supplies foo:bar the backend
     *     will return an unique path to the client which represent the lease (for [ "foo", "bar" ]
     *     the backend might return the Cloudname paths [ "foo", "bar", "uniqueid0" ],
     *     [ "foo", "bar", "uniqueid1" ] to clients acquiring the lease.
     *
     * @param data Temporary lease data. This is an arbitrary string supplied by the client. It
     *     carries no particular semantics for the backend and the backend only has to return the
     *     same string to the client. This value cannot be null.
     *
     * @return A LeaseHandle instance that the client can use to manipulate its data or release
     *     the lease (i.e. close it). The path to the lease can be accessed through this.
     */
    LeaseHandle createTemporaryLease(final CloudnamePath path, final String data);

    /**
     * Update a client's lease. Normally this is something the client does itself but libraries
     * built on top of the backends might use it to set additional properties.
     *
     * @param path path to the temporary lease
     * @param data the updated lease data
     * @return true if successful, false otherwise
     */
    boolean writeTemporaryLeaseData(final CloudnamePath path, final String data);

    /**
     * Read temporary lease data. Clients won't use this in regular use but rather monitor changes
     * through the listeners but libraries built on top of the backend might read the data.
     *
     * @param path path to the client lease
     * @return the data stored in the client lease
     */
    String readTemporaryLeaseData(final CloudnamePath path);

    /**
     * Add a listener to a set of temporary leases identified by a path. The temporary leases
     * doesn't have to exist but as soon as someone creates a lease matching the given path a
     * notification must be sent by the backend implementation.
     *
     * @param pathToWatch the path to observe for changes
     * @param listener client's listener. Callbacks on this listener will be invoked by the backend
     */
    void addTemporaryLeaseListener(final CloudnamePath pathToWatch, final LeaseListener listener);

    /**
     * Remove a previously attached listener. The backend will ignore leases that don't exist.
     *
     * @param listener the listener to remove
     */
    void removeTemporaryLeaseListener(final LeaseListener listener);

    /**
     * Create a permanent lease. A permanent lease persists even if the client that created it
     * terminates or closes the connection. Other clients will still see the lease. Permanent leases
     * must persist until they are explicitly removed.
     *
     * <p>All permanent leases must be unique. Duplicate permanent leases yield errors.
     *
     * @param path path to the permanent lease
     * @param data data to store in the permanent lease when it is created
     * @return true if successful
     */
    boolean createPermanantLease(final CloudnamePath path, final String data);

    /**
     * Remove a permanent lease. The lease will be removed and clients listening on the lease
     * will be notified.
     *
     * @param path the path to the lease
     * @return true if lease is removed
     */
    boolean removePermanentLease(final CloudnamePath path);

    /**
     * Update data on permanent lease.
     *
     * @param path path to the permanent lease
     * @param data data to write to the lease
     * @return true if successful
     */
    boolean writePermanentLeaseData(final CloudnamePath path, final String data);

    /**
     * Read data from permanent lease.
     *
     * @param path path to permanent lease
     * @return data stored in lease or null if the lease doesn't exist
     */
    String readPermanentLeaseData(final CloudnamePath path);

    /**
     * Add a listener to a permanent lease. The listener is attached to just one lease, as opposed
     * to the termporary lease listener.
     *
     * @param pathToObserve path to lease
     * @param listener callbacks on this listener is invoked by the backend
     */
    void addPermanentLeaseListener(final CloudnamePath pathToObserve, final LeaseListener listener);

    /**
     * Remove listener on permanent lease. Unknown listeners are ignored by the backend.
     * @param listener the listener to remove
     */
    void removePermanentLeaseListener(final LeaseListener listener);
}
