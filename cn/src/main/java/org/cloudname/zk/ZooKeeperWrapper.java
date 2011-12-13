package org.cloudname.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.cloudname.CloudnameException;
import org.cloudname.Coordinate;
import org.cloudname.ServiceStatus;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: dybdahl
 * Date: 12/12/11
 * Time: 8:45 AM
 * To change this template use File | Settings | File Templates.
 */
public class ZooKeeperWrapper implements  Watcher {

    // Instance variables
    private ZooKeeper zk;
    private String connectString;
    private static final Logger log = Logger.getLogger(ZooKeeperWrapper.class.getName());

    /**
     * Create a path in ZooKeeper.  We just start at the top and work
     * our way down.  Nodes that exist will throw an exception but we
     * will just ignore those.  The result should be a path consisting
     * of ZooKeeper nodes with the names specified by the path and
     * with their data element set to null.
     */
    public static void mkdir(ZooKeeper zk, String path, List<ACL> acl)  throws KeeperException  {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        String[] parts = path.split("/");

        String createPath = "";
        for (String p : parts) {
            createPath += "/" + p;
            try {
                zk.create(createPath, null, acl, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // This is okay.  Ignore.
            } catch (InterruptedException e) {
                // This may indicate a problem.
                throw new CloudnameException(e);
            }
        }
    }

    /**
     * Connect to ZooKeeper instance.
     *
     * TODO(borud, dybdahl): if the ZooKeeper server is not there this method
     *   will hang forever.  It should probably time out or produce an
     *   exception.
     *
     *   If one instance dies, it should reconnect to a different instance.
     *
     *
     * @param connectString the connect string of the ZooKeeper server
     *   connection string containing a comma separated list of host:port pairs
     */
    public ZooKeeperWrapper connect(String connectString) {
        this.connectString = connectString;

        try {
            zk = new ZooKeeper(connectString, SESSION_TIMEOUT, this);
            connectedSignal.await();
            log.info("Connected to ZooKeeper " + connectString);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return this;
    }




    @Override
    public void process(WatchedEvent event) {
        log.fine("Got event " + event.toString());

        // Initial connection to ZooKeeper is completed.
        if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
            if (connectedSignal.getCount() == 0) {
                // I am not sure if this can ever occur, but until I
                // know I'll just leave this log message in here.
                log.info("The connectedSignal count was already zero.  Duplicate Event.KeeperState.SyncConnected");
                return;
            }
            connectedSignal.countDown();
        }
    }

    /**
     * Close the connection to ZooKeeper.
     */
    public void close() {
        if (null == zk) {
            throw new IllegalStateException("Cannot close(): Not connected to ZooKeeper");
        }

        try {
            zk.close();
            log.info("ZooKeeper session closed for " + connectString);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    // Constants
    private static final int SESSION_TIMEOUT = 5000;
    private static final String CHARSET_NAME = "UTF-8";

    // This is the path prefix used by Cloudname in ZooKeeper.
    // Anything that lives under this prefix can only be touched by
    // the Cloudname library.  If clients begin to fiddle with nodes
    // under this prefix directly, all deals are off.
    private static final String CN_PATH_PREFIX = "/cn";

    

    // Latches that count down when ZooKeeper is connected
    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    public String getString(String path) {
        String statusPath = CN_PATH_PREFIX + "/" + path;

        try {
            Stat stat = new Stat();
            byte[] data = zk.getData(statusPath, null, stat);
            return new String(data, CHARSET_NAME));
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (UnsupportedEncodingException e) {
            // Should never happen.
            throw new CloudnameException(e);
        } catch (IOException e) {
            // TODO(borud): Contents of node must have been
            //   mangled. Throw more sensible exception.
            throw new CloudnameException(e);
        }    
    }

    
    public void deleteString(String path) {

    }
    
    public void setSting(String path, String data) {
        if (! open) {
            throw new IllegalStateException("Service handle was closed.");
        }

        try {
            Stat stat = zk.setData(statusPath,
                    status.toJson().getBytes(CHARSET_NAME),
                    lastStatusVersion);
            lastStatusVersion = stat.getVersion();
        } catch (KeeperException.BadVersionException e) {
            // TODO(borud): If we get this someone has been
            // fiddling with the status node and all bets are off.
            // Indicates major "breach of contract".
            throw new CloudnameException(e);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (UnsupportedEncodingException e) {
            // Shouldn't happen
            throw new CloudnameException(e);
        }

    }
    
    void trackPath(String path, NodeChangeClass nodeChange) {

    }
}
