package org.cloudname.zk;

import org.cloudname.CloudnameException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.cloudname.Coordinate;

import java.util.List;
import java.util.logging.Logger;

/**
 * Various ZooKeeper utilities.
 *
 * @author borud
 */
public class Util {
    public static final String CN_ENDPOINTS_NAME = "endpoints";
    public static final String CN_STATUS_NAME = "status";

    public static final String CN_CONFIG_NAME = "config";
    // Constants
    public static final int SESSION_TIMEOUT = 5000;
    public static final String CHARSET_NAME = "UTF-8";
    // This is the path prefix used by Cloudname in ZooKeeper.
    // Anything that lives under this prefix can only be touched by
    // the Cloudname library.  If clients begin to fiddle with nodes
    // under this prefix directly, all deals are off.
    public static final String CN_PATH_PREFIX = "/cn";


    private static final Logger log = Logger.getLogger(Util.class.getName());
    /**
     * Create a path in ZooKeeper.  We just start at the top and work
     * our way down.  Nodes that exist will throw an exception but we
     * will just ignore those.  The result should be a path consisting
     * of ZooKeeper nodes with the names specified by the path and
     * with their data element set to null.
     */
    public static void mkdir(ZooKeeper zk, String path, List<ACL> acl)
        throws KeeperException
    {
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
    public static String coordinateAsPath(String cell, String user, String service) {
        return cell + "/" + user + "/" + service;
    }
    
    public static String coordinateAsPath(String cell, String user, String service, Integer instance) {
        return coordinateAsPath(cell, user, service) + "/" + instance.toString();
    }
    
    public static String coordinateAsPath(Coordinate coordinate) {
        return coordinateAsPath(coordinate.getCell(), coordinate.getUser(), coordinate.getService(),
                   coordinate.getInstance());
    }
}