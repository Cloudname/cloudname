package org.cloudname.core;

/**
 * Backends implement this interface. Clients won't use this interface; the logic is handled by the
 * libaries built on top of the backend. Each backend provides a few basic primitives that must be
 * implemented. One caveat: The backend is responsible for cleaning up unused paths. The clients won't
 * remote unused elements.
 *
 * There are two kinds of leases - permanent and temporary. The permanent leases persist in the
 * backend and aren't removed when clients disconnect, even if *all* clients disconnects.
 * The temporary leases are removed by the backend when the client closes. Note that clients might
 * not be well-behaved and may terminate without calling close(). The backend should remove
 * these leases automatically.
 *
 * Clients listen on both kinds of leases and get notifications through listeners whenever something
 * is changed. Notifications to the clients are sent in the same order they are received.
 *
 * Each lease have a data string attached to the lease and clients may update this freely.
 *
 * @author stalehd@gmail.com
 */
public interface CloudnameBackend extends AutoCloseable {
    /**
     * Create a temporary lease. The temporary lease is limited by the client's connection and will
     * be available for as long as the client is connected to the backend. Once the client
     * disconnects (either through the LeaseHandle instance that is returned or just vanishing
     * from the face of the earth) the lease is removed by the backend. The backend should support
     * an unlimited number of leases (FSVO "unlimited")
     *
     * @param path Path to temporary lease. This value cannot be null. The path supplied by the
     *     client is just the stem of the full lease, i.e. if a client supplies foo:bar the backend
     *     will return an unique path to the client which represent the lease (for "foo:bar" the
     *     backend might return "foo:bar:uniqueid0", "foo:bar:uniqueid1"... to clients acquiring
     *     the lease.
     *
     * @param data Temporary lease data. This is an arbitrary string supplied by the client. It
     *     carries no particular semantics for the backend and the backend only have to return the
     *     same string to the client. This value cannot be null.
     *
     * @return A LeaseHandle instance that the client can use to manipulate its data or release
     *     the lease (ie closing it).
     */
    LeaseHandle createTemporaryLease(final CloudnamePath path, final String data);

    /**
     * Update a client's lease. Normally this is something the client does itself but libraries
     * built on top of the backends might use it to set additional properties.
     * @param path Path to the temporary lease.
     * @param data The updated lease data.
     * @return True if successful, false otherwise
     */
    boolean writeTemporaryLeaseData(final CloudnamePath path, final String data);

    /**
     * Read temporary lease data. Clients won't use this in regular use but rather monitor changes
     * through the listeners but libraries built on top of the backend might read the data.
     *
     * @param path Path to the client lease.
     * @return The data stored in the client lease.
     */
    String readTemporaryLeaseData(final CloudnamePath path);

    /**
     * Add a listener to a set of temporary leases identified by a path. The temporary leases
     * doesn't have to exist but as soon as someone creates a lease matching the given path a
     * notification must be sent by the backend implementation.
     *
     * @param pathToObserve The path to observe for changes.
     * @param listener Client's listener. Callbacks on this listener will be invoked by the backend.
     */
    void addTemporaryLeaseListener(final CloudnamePath pathToObserve, final LeaseListener listener);

    /**
     * Remove a previously attached listener. The backend will ignore leases that doesn't exist.
     *
     * @param listener The listener to remove
     */
    void removeTemporaryLeaseListener(final LeaseListener listener);

    /**
     * Create a permanent lease. A permanent lease persists even if the client that created it
     * terminates or closes the connection. Other clients will still see the lease. Permanent leases
     * must persist until they are explicitly removed.
     *
     * All permanent leases must be unique. Duplicate permanent leases yields an error.
     *
     * @param path Path to the permanent lease.
     * @param data Data to store in the permanent lease when it is created.
     * @return true if successful
     */
    boolean createPermanantLease(final CloudnamePath path, final String data);

    /**
     * Remove a permanent lease. The lease will be removed and clients listening on the lease
     * will be notified.
     *
     * @param path The path to the lease
     * @return true if lease is removed.
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
     * @param pathToObserver Path to lease
     * @param listener Listener. Callbacks on this listener is invoked by the backend.
     */
    void addPermanentLeaseListener(final CloudnamePath pathToObserver, final LeaseListener listener);

    /**
     * Remove listener on permanent lease. Unknown listeners are ignored by the backend.
     * @param listener The listener to remove
     */
    void removePermanentLeaseListener(final LeaseListener listener);
}
