package org.cloudname.a3.storage;

import org.cloudname.a3.domain.ServiceCoordinate;
import org.cloudname.a3.domain.User;
import org.cloudname.a3.domain.UserDB;

import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;

/**
 * A memory based password database.  Does not support updates, but
 * does support being initialized from an InputStream so we can use it
 * for testing and/or file based userDBs.
 *
 * @author borud
 */
public class MemoryStorage
    implements UserDBStorage
{
    private boolean opened = false;
    private boolean closed = false;
    private UserDB userDB = null;


    public static MemoryStorage fromReader(final Reader reader)
        throws IOException
    {
        final String json;
        {
            final StringBuilder sb = new StringBuilder();
            final BufferedReader bufferedReader = new BufferedReader(reader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                sb.append(line);
            }
            json = sb.toString();
        }

        final UserDB userDB = UserDB.fromJson(json);
        final MemoryStorage mstore = new MemoryStorage();
        mstore.open();
        mstore.createUserDB(userDB);
        return mstore;
    }

    /**
     * @deprecated does not specify how to convert bytes to
     * characters, replaced by {link #fromReader()}.
     */
    public static MemoryStorage fromInputStream(final InputStream is)
        throws IOException
    {
        return fromReader(new InputStreamReader(is));
    }

    /**
     * Open the storage.  For the most part this implementation only
     * has some state checks to make the implementation more useful
     * for identifying sloppy state handling in unit tests.
     */
    @Override
    public void open() {
        if (closed) {
            throw new IllegalStateException("Cannot re-open a closed storage");
        }

        if (opened) {
            throw new IllegalStateException("Called open() on already opened storage");
        }

        opened = true;
    }

    /**
     * Close the storage.  For the most part this implementation only
     * has some state checks to make the implementation more useful
     * for identifying sloppy state handling in unit tests.
     */
    @Override
    public void close() {
        if (closed) {
            throw new IllegalStateException("Called close() on already closed storage");
        }

        if (! opened) {
            throw new IllegalStateException("Called close() on storage that was never opened");
        }

        closed = true;
    }

    @Override
    public void registerUserDBWatcher(Watcher watcher) {
        // Since this implementation does not maintain a persistent
        // storage this method is going to be a no-op.
    }

    @Override
    public void createUserDB(UserDB db) {
        ensureOpen();

        // If we already have a UserDB then clearly "create" should fail
        if (null != userDB) {
            throw new A3StorageException("UserDB already exists");
        }

        // The first version is always version 0
        db.setVersion(0);
        userDB = db;
    }

    @Override
    public void updateUserDB(UserDB db) {
        ensureOpen();

        if (null == userDB) {
            throw new A3StorageException("The userDB does not exist and thus cannot be updated");
        }

        // If the version of the new user database does not match that
        // of the one we have we have a problem.
        if (db.getVersion() != userDB.getVersion()) {
            throw new A3StorageException("Version numbers do not match. Got "
                                         + db.getVersion() + " but expected "
                                         + userDB.getVersion());
        }

        // Version numbers match, which means we now bump the version number
        // of the userDB we have "stored".
        db.setVersion(db.getVersion() + 1);
        userDB = db;
    }

    @Override
    public UserDB getUserDB() {
        ensureOpen();
        return userDB;
    }

    /**
     * Make sure the MemoryStorage is open and not closed.
     */
    private void ensureOpen() {
        if (!opened) {
            throw new A3StorageException("The MemoryStorage was never opened");
        }

        if (closed) {
            throw new A3StorageException("The MemoryStorage was closed");
        }
    }
}