package org.cloudname.a3;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import javax.security.auth.Subject;

import org.cloudname.a3.domain.User;
import org.cloudname.a3.domain.UserDB;
import org.cloudname.a3.storage.MemoryStorage;
import org.cloudname.a3.storage.UserDBStorage;
import org.joda.time.DateTime;

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
     * Gets a collection of Users by looking up a specific property and matching on of it's values
     * (All properties are whitespace separated lists).
     *
     * @param key the property key
     * @param value the value to look for
     * @return a GetUserResult instance containing a collection of Users.
     */
    public GetUserResult getUserByProperty(final String key, final String value) {
        final UserDB userDB;
        try {
            userDB = getUserDb();
        } catch (Exception e) {
            return new GetUserResult(GetUserResult.State.INTERNAL_ERROR,
                    "Unable to get user database",
                    e);
        }
        final Set<User> users = new HashSet<User>();
        for (final User user : userDB.getAllUsers()) {
            final Map<String, String> properties = user.getProperties();
            if (properties == null || properties.get(key) == null) {
                continue;
            }
            final String propertyValue = properties.get(key);
            if (Arrays.asList(propertyValue.split(" ")).contains(value)) {
                users.add(user);
            }
        }
        if (users.isEmpty()) {
            return new GetUserResult(GetUserResult.State.UNKNOWN_USER);
        }
        return new GetUserResult(users, GetUserResult.State.OK);
    }

    private UserDB getUserDb() throws Exception {
        // If we do not already have a user database we fetch it.
        if (null == dbReference.get()) {
            // Tempted to use compareAndSet but this doesn't really make
            // that much sense since the value we fetch is likely to be
            // newer anyway.  I think.
            dbReference.set(dbStorage.getUserDB());
        }
        return dbReference.get();
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

        // Deal with the case where the user does not exist.
        final User user;
        try {
            user = getUserDb().getUser(username);
        } catch (Exception e) {
            return new AuthnResult(AuthnResult.State.INTERNAL_ERROR,
                    "Unable to get user database",
                    e);
        }
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
        if (a3Principals.isEmpty()) {
            return null;
        }
        if (a3Principals.size() > 1) {
            throw new RuntimeException("Fishy fishy: " + a3Principals);
        }
        final A3Principal principal = a3Principals.iterator().next();
        return principal.getAuthenticatedUser();
    }
}
