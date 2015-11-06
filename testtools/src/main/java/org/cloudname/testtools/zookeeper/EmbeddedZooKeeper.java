package org.cloudname.testtools.zookeeper;

import org.apache.curator.test.TestingServer;

import java.io.File;
import java.io.IOException;

/**
 * Utility class to fire up an embedded ZooKeeper server in the
 * current JVM for testing purposes.
 *
 * @author borud
 * @author stalehd
 */
public final class EmbeddedZooKeeper {
    private final File rootDir;
    private final int port;
    private TestingServer server;

    /**
     * @param rootDir the root directory of where the ZooKeeper
     *   instance will keep its files.  If null, a temporary directory is created
     * @param port the port where ZooKeeper will listen for client
     *   connections.
     */
    public EmbeddedZooKeeper(File rootDir, int port) {
        this.rootDir = rootDir;
        this.port = port;
    }

    private void delDir(File path) throws IOException {
        for(File f : path.listFiles())
        {
            if(f.isDirectory()) {
                delDir(f);
            } else {
                if (!f.delete() && f.exists()) {
                    throw new IOException("Failed to delete file " + f);
                }
            }
        }
        if (!path.delete() && path.exists()) {
            throw new IOException("Failed to delete directory " + path);
        }

    }

    /**
     * Delete all data owned by the ZooKeeper instance.
     * @throws IOException if some file could not be deleted
     */
    public void del() throws IOException {
        File  path = new File(rootDir, "data");
        delDir(path);
    }


    /**
     * Set up the ZooKeeper instance.
     */
    public void init() throws Exception {
        this.server = new TestingServer(this.port, this.rootDir);
        // Create the data directory
        File  dataDir = new File(rootDir, "data");
        dataDir.mkdir();

        this.server.start();
    }

    /**
     * Shut the ZooKeeper instance down.
     * @throws IOException if shutdown encountered I/O errors
     */
    public void shutdown() throws IOException {
        this.server.stop();
        del();
    }

    /**
     * Get the client connection string for the ZooKeeper instance.
     *
     * @return a String containing a comma-separated list of host:port
     *   entries for use as a parameter to the ZooKeeper client class.
     */
    public String getClientConnectionString() {
        return "127.0.0.1:" + port;
    }
}
