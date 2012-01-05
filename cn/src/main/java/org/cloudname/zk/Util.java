package org.cloudname.zk;

import org.apache.zookeeper.data.Stat;
import org.cloudname.CloudnameException;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.KeeperException;
import org.cloudname.Coordinate;

import java.util.List;
import java.util.Vector;
import java.util.logging.Logger;

/**
 * Various ZooKeeper utilities.
 *
 * @author borud
 */
public class Util {
    // Constants
    public static final String CHARSET_NAME = "UTF-8";

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

    /**
     * Figures out if there are sub nodes under the path. If the path does not exist, returns false.
     * @param zk
     * @param path
     * @param acl
     * @return true iff the node exists and has children.
     */
    public static boolean existAndHasChildren(ZooKeeper zk, String path, List<ACL> acl)  {
        if (getVersionForDeletion(zk, path, acl) == null) {
            return false;
        }

        List<String> children = null;
        try {
            children = zk.getChildren(path, false);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        }
        return ((children != null) && (children.size() > 0));
    }

    /**
     * Returns the version of the path, if not exist, return null.
     * @param zk
     * @param path
     * @param acl
     * @return version number or null.
     */
    public static Integer getVersionForDeletion(ZooKeeper zk, String path, List<ACL> acl) {

        Stat stat = null;
        try {
            stat = zk.exists(path, false);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
        if (stat == null) {
            return null;
        }
        return stat.getVersion();
    }

    /**
     * Deletes as much as possible from a path, i.e. all nodes until there is some fan-out due to other nodes.
     * @param zk
     * @param path
     * @param acl
     * @return the numbner of deletes nodes.
     */
    public static int deleteAsMuchAsPossible(ZooKeeper zk, String path, List<ACL> acl) {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        String[] parts = path.split("/");

        // We are happy if only the first two deletions went through. The other deletions are just cleaning up if
        // there are no more coordinates on the same rootPath.
        int deletedNodes = 0;
        Vector<String> paths = new Vector<String>();
        String incrementalPath = "";
        for (String p : parts) {
            incrementalPath += "/" + p;
            paths.add(incrementalPath);
        }

        for (int counter = paths.size() - 1; counter >= 0; counter--) {
            String deletePath = paths.elementAt(counter);
            Integer version = getVersionForDeletion(zk, deletePath, acl);
            if (version == null || version < 0) {
                throw new CloudnameException(new RuntimeException("Could not get version for path " + deletePath));
            }
            if (existAndHasChildren(zk, deletePath, acl)) {
                return deletedNodes;
            }
            try {
                zk.delete(deletePath, version);
            } catch (KeeperException e) {
                throw new CloudnameException(e);
            } catch (InterruptedException e) {
                throw new CloudnameException(e);
            }
            deletedNodes++;
        }
        return deletedNodes;
    }
 }