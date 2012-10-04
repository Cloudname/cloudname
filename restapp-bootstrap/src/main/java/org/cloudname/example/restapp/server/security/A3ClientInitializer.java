package org.cloudname.example.restapp.server.security;

import com.google.common.io.Closeables;
import org.cloudname.a3.A3Client;
import org.cloudname.flags.Flag;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class for creating a3 client form a user database and setting it on the {@link AuthenticationFilter}.
 * <p>
 * The user db file or (class) path should be set by Base based on command-line options.
 */
public enum A3ClientInitializer {
    ;

    @Flag(name = "a3-client-path", description = "Classpath to an A3 user database file(usually called clinets.json); start with /")
    private static String userDbPath = "/client.json";

    @Flag(name = "a3-client-file", description = "Path to an A3 user database file (usually called clinets.json)")
    private static String userDbFile = null;

    private static final Logger log = Logger.getLogger(A3Client.class.getName());

    /**
     * Try to locate A3 user database and create an A3Client from it and set it on the authorization filter so that it
     * is during by REST requests. If no user database was specified/found then an empty one will be used (and nobody
     * will be able to authorize).
     */
    public static void tryInitializeA3Client() {
        try {
            A3Client a3Client = createA3Client();
            AuthenticationFilter.setA3Client(a3Client);
        } catch (IOException e) {
            throw new RuntimeException("Failure during A3 Client initialization", e);
        }
    }

    private static A3Client createA3Client() throws IOException {
        A3Client client;
        if (userDbFile != null) {
            client = loadFromFile(userDbFile);
            log.info("User database loaded from the file " + userDbFile);
        } else {
            log.fine("No user database file provided, trying the class path resource" + userDbPath);
            client = loadFromClasspath(userDbPath);
            if (client == null) {
                log.info("No A3 client, user's won't be able to access secured resources (if any) - " +
                        "neither a3-client-file provided nor " + userDbPath + " found on the class path");
                client = createEmptyClient();
            } else {
                log.info("User database loaded from th classpath " + userDbPath);
            }
        }

        // Must open the client before use
        client.open();

        return client;
    }

    /**
     * Creates an a3client without any users.
     *
     * @return the created a3client. <code>null</code> is returned if the client could not be created.
     */
    private static A3Client createEmptyClient() {
        final String emptyUserDbJson = "[]";
        final Reader reader = new StringReader(emptyUserDbJson);
        try {
            return A3Client.newMemoryOnlyClient(reader);
        } catch (IOException e) {
            throw new RuntimeException("Unexpected", e);
        }
    }

    /**
     * Initialize the a3client using the json file located at the given path on the classpath. See
     * {@link ClassLoader#getResource} for details of how the path is resolved.
     *
     * @param path
     *            the path to the resource.
     * @return the created a3client. <code>null</code> is returned if the client could not be created or if the resource
     *         doesn't exist
     */
    private static A3Client loadFromClasspath(String path) {
        final URL resource = A3ClientInitializer.class.getResource(path);

        if (resource == null) {
            log.log(Level.FINE, "Didn't find resource " + path + " on the class path");
            return null;
        } else {
            log.fine("Loading a3 user db from the classpath at " + resource);
        }

        try {
            InputStream is = resource.openStream();
            return loadFromReader(new InputStreamReader(is));
        } catch (IOException e) {
            log.log(Level.WARNING, "Failed to load daa from the classpath " + path, e);
            return null;
        }
    }

    /**
     * Initialize the a3client using the json file located at the given path.
     *
     * @param filename
     *            the name of the file to load.
     * @return the created a3client.
     * @throws IOException
     *             if the file could not be loaded.
     */
    private static A3Client loadFromFile(final String filename) throws IOException {
        final Charset charset = Charset.forName("UTF-8");
        final InputStream stream = new FileInputStream(filename);
        final Reader reader = new InputStreamReader(stream, charset);

        return loadFromReader(reader);
    }

    private static A3Client loadFromReader(final Reader reader) throws IOException {
        try {
            return A3Client.newMemoryOnlyClient(reader);
        } finally {
            Closeables.closeQuietly(reader);
        }
    }

    public static void setUserDbPathForTesting(String userDbPath) {
        A3ClientInitializer.userDbPath = userDbPath;
    }
}
