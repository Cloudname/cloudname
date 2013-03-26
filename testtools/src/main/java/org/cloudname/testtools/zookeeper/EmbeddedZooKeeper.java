package org.cloudname.testtools.zookeeper;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import java.io.File;
import java.io.IOException;

/**
 * Utility class to fire up an embedded ZooKeeper server in the
 * current JVM for testing purposes.
 *
 * @author borud
 */
public final class EmbeddedZooKeeper {
    private File rootDir;
    private int port;
    private NIOServerCnxn.Factory cnxnFactory;
    private FileTxnSnapLog fileTxnSnapLog;

    /**
     * Make default constructor private.
     */
    private EmbeddedZooKeeper() {}

    /**
     * @param rootDir the root directory of where the ZooKeeper
     *   instance will keep its files.  If null, a temporary directory is created
     * @param port the port where ZooKeeper will listen for client
     *   connections.
     */
    public EmbeddedZooKeeper(File rootDir, int port) {
        if (rootDir == null) {
            rootDir = createTempDir();
        }

        this.rootDir = rootDir;
        this.port = port;

        if (! rootDir.exists()) {
            throw new IllegalStateException("Root directory does not exist: " + rootDir);
        }
    }

    /**
     * Delete directory with content.
     * @param path to be deleted.
     */
    static private void deleteDirectory(File path) {
        for(File f : path.listFiles())
        {
            if(f.isDirectory()) {
                deleteDirectory(f);
                f.delete();
            } else {
                f.delete();
            }
        }
        path.delete();
    }


   /**
     * Deletes and recreates a temp dir. Sets deleteOnExit().
     * @return
     */
    private static File createTempDir() {
        File baseDir = new File(System.getProperty("java.io.tmpdir"));
        File tempDir = new File(baseDir, "EmbeddedZooKeeper");
        if (tempDir.exists()) {
            System.err.println("Deleting old instance on startup.");
            deleteDirectory(tempDir);
        }
        tempDir.mkdir();
        tempDir.deleteOnExit();
        return tempDir;
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
        // Create the data directory
        File  dataDir = new File(rootDir, "data");
        dataDir.mkdir();

        // Create server
        ServerConfig config = new ServerConfig();
        config.parse( new String[] {Integer.toString(port), dataDir.getCanonicalPath()});
        ZooKeeperServer zk = new ZooKeeperServer();
        fileTxnSnapLog = new FileTxnSnapLog(new File(config.getDataLogDir()),
                new File(config.getDataDir()));
        zk.setTxnLogFactory(fileTxnSnapLog);
        zk.setTickTime(config.getTickTime());
        zk.setMinSessionTimeout(config.getMinSessionTimeout());
        zk.setMaxSessionTimeout(config.getMaxSessionTimeout());

        cnxnFactory = new NIOServerCnxn.Factory(config.getClientPortAddress(),
                                                config.getMaxClientCnxns());
        cnxnFactory.startup(zk);
    }

    /**
     * Shut the ZooKeeper instance down.
     * @throws IOException if shutdown encountered I/O errors
     */
    public void shutdown() throws IOException {
        if (null != cnxnFactory) {
            cnxnFactory.shutdown();
            cnxnFactory = null;
        }
        if (null != fileTxnSnapLog) {
            fileTxnSnapLog.close();
            fileTxnSnapLog = null;
        }
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