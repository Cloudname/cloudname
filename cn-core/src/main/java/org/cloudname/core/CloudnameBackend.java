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
     * Create a lease.The temporary lease is limited by the client's connection and will
     * be available for as long as the client is connected to the backend. Once the client
     * disconnects (either through the LeaseHandle instance that is returned or just vanishing
     * from the face of the earth) the lease is removed by the backend. The backend should support
     * an unlimited number of leases (FSVO "unlimited").
     *
     * @param type Type of lease. This value cannot be null.
     *
     * @param path The full path to lease. This value cannot be null.
     *
     * @param data Lease data. This is an arbitrary string supplied by the client. It
     *     carries no particular semantics for the backend and the backend only has to return the
     *     same string to the client. This value cannot be null.
     *
     * @return A LeaseHandle instance that the client can use to manipulate its data or release
     *     the lease (i.e. close it).
     */
    LeaseHandle createLease(LeaseType type, CloudnamePath path, String data);

    /**
     * Remove a lease. The lease will be removed and clients listening on the lease
     * will be notified. Leases with the {@link LeaseType} set to PERMANENT can be removed by any
     * client. Leases with the {@link LeaseType} set to TEMPORARY can only be removed by the owner
     * (ie creator) of the lease.
     *
     * @param path the path to the lease
     * @return true if lease is removed
     */
    boolean removeLease(final CloudnamePath path);

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

    /**
     * Update a client's lease.
     *
     * @param path path to the lease
     * @param data the updated lease data
     * @return true if successful, false otherwise
     */
    boolean writeLeaseData(final CloudnamePath path, final String data);

    /**
     * Read temporary lease data. Clients won't use this in regular use but rather monitor changes
     * through the listeners but libraries built on top of the backend might read the data.
     *
     * @param path path to the client lease
     * @return the data stored in the client lease
     */
    String readLeaseData(final CloudnamePath path);

    /**
     * Add a listener to a set of leases identified by a path. As soon as someone creates a lease
     * matching the given path a notification is be sent by the backend.
     *
     * @param pathToWatch the path to observe for changes
     * @param listener client's listener. Callbacks on this listener will be invoked by the backend
     */
    void addLeaseCollectionListener(final CloudnamePath pathToWatch, final LeaseListener listener);

    /**
     * Listen to a single lease.
     *
     * @param pathToObserve path to lease
     * @param listener callbacks on this listener is invoked by the backend
     */
    void addLeaseListener(final CloudnamePath pathToObserve, final LeaseListener listener);

    /**
     * Remove listener on permanent lease. Unknown listeners are ignored by the backend.
     * @param listener the listener to remove
     */
    void removeLeaseListener(final LeaseListener listener);
}
