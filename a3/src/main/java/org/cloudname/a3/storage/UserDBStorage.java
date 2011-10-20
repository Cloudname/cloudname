package org.cloudname.a3.storage;

import org.cloudname.a3.domain.User;
import org.cloudname.a3.domain.UserDB;

/**
 * Interface for UserDB storage.
 *
 * @author borud
 */
public interface UserDBStorage {

    /**
     * This interface defines the API for asynchronous updates of the
     * user database.  See registerUserDBWatcher().
     */
    public static interface Watcher {
        /**
         * @param userDB updated userDB instance.
         */
        public void onUpdate(UserDB userDB);
    }

    /**
     * Open the storage for use.  Performs any necessary
     * initialization before we can access the storage.
     */
    public void open();

    /**
     * Close the storage.  After close has been called no further
     * operations against the storage can take place.  Re-opening a
     * closed storage has undefined behavior.
     */
    public void close();

    /**
     * Register a Watcher which will be called whenever we discover
     * that the database has been updated in the underlying storage
     * mechanism.
     */
    public void registerUserDBWatcher(Watcher watcher);

    /**
     * Create a user database and populate it with the contents of a UserDB.
     *
     * @param db the UserDB instance we wish to persist to a user database.
     */
    public void createUserDB(UserDB db);

    /**
     * Update an existing user database.
     *
     * Note that the version of the UserDB, as reported by the
     * getVersion() call, must match the version which is stored.
     * When a UserDB is updated its version is incremented.
     *
     * <p> Note that it is not a given that the implementation will
     * make a defensive copy of the UserDB you give it. (For example
     * the MemoryStorage implementation does NOT).
     *
     * @param db the UserDB we wish to store as an update
     */
    public void updateUserDB(UserDB db);

    /**
     * Get the UserDB.
     *
     * @return get the last version of the UserDB from storage.
     */
    public UserDB getUserDB();
}