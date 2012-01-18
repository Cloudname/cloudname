package org.cloudname.zk;

import org.cloudname.CloudnameException;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.List;
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
            // Sonar will complain about this.  Usually it would be
            // right but in this case it isn't.
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
     * Lists sub nodes
     * @param zk
     * @param path starts from this path
     * @param acl
     * @param nodeList put sub-nodes in this list
     */
    public static void listRecursively(ZooKeeper zk, String path, List<ACL> acl, List<String> nodeList) {
        List<String> children = null;
        try {
            children = zk.getChildren(path, false);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        }
        if (children.size() == 0) {
            nodeList.add(path);
            return;
        }
        for (String childPath : children) {
            listRecursively(zk, path + "/" +childPath, acl, nodeList);
        }
    }
    
    /**
     * Figures out if there are sub-nodes under the path.
     * @param zk
     * @param path
     * @return true iff the node exists and has children.
     */
    public static boolean hasChildren(ZooKeeper zk, String path)  {
        if (! exist(zk, path)) {
            throw new CloudnameException(
                    new RuntimeException("Could not get children due to non-existing path " + path));
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
     * Figures out if a path exists.
     * @param zk
     * @param path
     * @return true if the path exists.
     */
    public static boolean exist(ZooKeeper zk, String path)  {
        try {
            return zk.exists(path, false) != null;
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
    }
    
    /**
     * Returns the version of the path.
     * @param zk
     * @param path
     * @return version number 
     */
    public static int getVersionForDeletion(ZooKeeper zk, String path) {

        try {
            int version = zk.exists(path, false).getVersion();
            if (version < 0) {
                throw new CloudnameException(new RuntimeException("Got negative version for path " + path));
            }
            return version;
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
    }

    /**
     * Deletes nodes from a path from the right to the left.
     * @param zk
     * @param path to be deleted
     * @param keepMinLevels is the minimum number of levels (depths) to keep in the path.
     * @return the number of deleted levels.
     */
    public static int deletePathKeepRootLevels(ZooKeeper zk, String path, int keepMinLevels) {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        String[] parts = path.split("/");

        // We are happy if only the first two deletions went through. The other deletions are just cleaning up if
        // there are no more coordinates on the same rootPath.
        int deletedNodes = 0;
        List<String> paths = new ArrayList<String>();
        String incrementalPath = "";
        for (String p : parts) {
            incrementalPath += "/" + p;
            paths.add(incrementalPath);
        }

        for (int counter = paths.size() - 1; counter >= keepMinLevels; counter--) {
            String deletePath = paths.get(counter);
            int version = getVersionForDeletion(zk, deletePath);
            if (hasChildren(zk, deletePath)) {
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
