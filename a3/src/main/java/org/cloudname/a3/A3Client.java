package org.cloudname.a3;

import org.cloudname.a3.domain.ServiceCoordinate;
import org.cloudname.a3.domain.User;
import org.cloudname.a3.domain.UserDB;

import org.cloudname.a3.storage.UserDBStorage;
import org.cloudname.a3.storage.ZKStorage;
import org.cloudname.a3.storage.MemoryStorage;

import org.joda.time.DateTime;

import java.io.InputStream;
import java.io.IOException;
import java.io.Reader;

import java.security.AccessController;
import java.security.AccessControlContext;
import java.security.Principal;

import java.util.Set;

import java.util.concurrent.atomic.AtomicReference;

import java.util.logging.Logger;
import java.util.logging.Level;

import javax.security.auth.Subject;

/**
 * A3 client interface.
 *
 * @author borud
 */
public class A3Client
{
    private static final Logger log = Logger.getLogger(A3Client.class.getName());
    private UserDBStorage dbStorage;
    private AtomicReference<UserDB> dbReference = new AtomicReference<UserDB>();
    private boolean opened = false;


    /**
     * @param dbStorage the UserDBStorage this client should use.
     */
    public A3Client(UserDBStorage dbStorage) {
        this.dbStorage = dbStorage;
    }

    /**
     * Open the client for use.
     */
    public void open() {
        dbStorage.registerUserDBWatcher(
            new UserDBStorage.Watcher() {
                @Override
                public void onUpdate(final UserDB userDB) {
                    dbReference.set(userDB);
                }
            });
        opened = true;
    }

    /**
     * Close the client.  After the client has been closed it cannot
     * be reopened.
     */
    public void close() {
        if (null == dbStorage) {
            return;
        }
        dbStorage.close();
        dbStorage = null;
    }

    private void ensureOpened() {
        if (! opened) {
            throw new IllegalStateException("The a3 client was not open()'ed");
        }
    }

    /**
     * Create new client that uses the ZooKeeper storage.
     *
     * @param coordinate The service coordinate of the user database.
     * @param connectString the ZooKeeper connect string.
     * @return an A3Client instance backed by ZooKeeper storage
     */
    public static A3Client newZooKeeperClient(ServiceCoordinate coordinate, String connectString) {
        ZKStorage zs = new ZKStorage(coordinate, connectString);
        zs.open();
        return new A3Client(zs);
    }

    /**
     * Create new client that uses MemoryStorage and is initialized
     * from an InputStream.
     *
     * @param in an InputStream which contains valid JSON user database.
     * @return an A3Client instance backed by MemoryStorage
     * @deprecated does not specify how to convert bytes to
     * characters, use a Reader instead of InputStream.
     */
    public static A3Client newMemoryOnlyClient(InputStream in) throws IOException {
        return new A3Client(MemoryStorage.fromInputStream(in));
    }

    /**
     * Create new client that uses MemoryStorage and is initialized
     * from a Reader.
     *
     * @param in a Reader which contains a valid JSON user database.
     * @return an A3Client instance backed by MemoryStorage
     */
    public static A3Client newMemoryOnlyClient(Reader in) throws IOException {
        return new A3Client(MemoryStorage.fromReader(in));
    }


    /**
     * Authenticate the user.  If the authentication succeeds the
     * AuthnResult will have a valid user.
     *
     * Typical usage:
     *
     *    AuthnResult r = client.authenticate("someuser", "somepassword");
     *    if (! r.isOk()) {
     *      // Deal with authentication failure
     *      return;
     *    }
     *
     * @param username the username of the user we wish to authenticate
     * @param cleartextPassword the cleartext password we wish to verify
     * @return an AuthnResult
     */
    public AuthnResult authenticate(String username, String cleartextPassword) {
        ensureOpened();

        // If we do not already have a user database we fetch it.
        if (null == dbReference.get()) {
            try {
                // Tempted to use compareAndSet but this doesn't really make
                // that much sense since the value we fetch is likely to be
                // newer anyway.  I think.
                dbReference.set(dbStorage.getUserDB());
            } catch (Exception e) {
                return new AuthnResult(AuthnResult.State.INTERNAL_ERROR,
                                       "Unable to get user database",
                                       e);
            }
        }

        // Deal with the case where the user does not exist.
        final User user = dbReference.get().getUser(username);
        if (null == user) {
            return new AuthnResult(AuthnResult.State.UNKNOWN_USER, "Unknown user");
        }

        // The user exists. Try to match the password.
        if (Password.matchSecret(cleartextPassword, user.getPassword())) {
            // Allrightey!
            return new AuthnResult(user, AuthnResult.State.OK);
        }

        // The password didn't match. Does the user have an 'old' password?
        if (user.getOldPassword() == null) {
            // Nope. Done.
            return new AuthnResult(AuthnResult.State.WRONG_PASSWORD, "Incorrect password");
        }

        // Old password exists. But has it expired?
        if (new DateTime().compareTo(user.getOldPasswordExpiry()) >= 0) {
            // Expired, so we don't use it.
            return new AuthnResult(AuthnResult.State.WRONG_PASSWORD, "Incorrect password");
        }

        // Old password is not expired. Try to match it.
        if (Password.matchSecret(cleartextPassword, user.getOldPassword())) {
            // Allrightey! (But the client should start using the new password.)
            log.warning("User " + username + " authenticated with old password. Client should be notified.");
            return new AuthnResult(user, AuthnResult.State.OK);
        }

        // Nothing left to try. We failed.
        return new AuthnResult(AuthnResult.State.WRONG_PASSWORD, "Incorrect password");
    }

    public static User getCurrentAuthenticatedUser()
    {
        final AccessControlContext acc = AccessController.getContext();
        final Subject subject = Subject.getSubject(acc);
        if (subject == null) {
            return null;
        }
        final Set<A3Principal> a3Principals
            = subject.getPrincipals(A3Principal.class);
        if (a3Principals == null || a3Principals.isEmpty()) {
            return null;
        }
        if (a3Principals.size() > 1) {
            throw new RuntimeException("Fishy fishy: " + a3Principals);
        }
        final A3Principal principal = a3Principals.iterator().next();
        return principal.getAuthenticatedUser();
    }
}