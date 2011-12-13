package org.cloudname.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.cloudname.*;

import java.io.UnsupportedEncodingException;
import java.util.logging.Logger;


/**
 * A service handle implementation.
 *
 * @author borud
 */
public class ZkServiceHandle implements ServiceHandle {
    private Coordinate coordinate;
    private volatile boolean open = true;
    private int lastStatusVersion = -1;
    private String prefix;
    private String statusPath;
    private String endpointsPath;
    private String configPath;

    private ZooKeeper zk;

    private static final Logger log = Logger.getLogger(ZkServiceHandle.class.getName());


    /**
     * Create a ZkServiceHandle for a given coordinate.
     *
     * This constructor is slightly evil since it does IO, but if
     * any of the IO operations fail the object is irrelevant
     * anyway.
     *
     * TODO(borud): expand error handling.
     *
     * @param coordinate the coordinate for this service handle.
     */
    public ZkServiceHandle(Coordinate coordinate, ZooKeeper zk) {


        this.coordinate = coordinate;
        this.zk = zk;
        // Just set some paths for convenience
        prefix = prefix = Util.CN_PATH_PREFIX + "/" + Util.coordinateAsPath(coordinate);
        statusPath = prefix + "/" + Util.CN_STATUS_NAME;
        endpointsPath = prefix + "/" + Util.CN_ENDPOINTS_NAME;
        configPath = prefix + "/" + Util.CN_CONFIG_NAME;

        // Stat the status node so we have the version.  If later
        // we try to operate on the status node and we do not have
        // the correct version this can mean that someone else has
        // been meddling with the status node.  In which case we
        // must complain loudly.
        try {
            Stat stat = zk.exists(statusPath, false);
            lastStatusVersion = stat.getVersion();
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
    }

    @Override
    public void setStatus(ServiceStatus status) {
        if (! open) {
            throw new IllegalStateException("Service handle was closed.");
        }

        try {
            Stat stat = zk.setData(statusPath,
                    status.toJson().getBytes(Util.CHARSET_NAME),
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

    @Override
    public void putEndpoint(String name, Endpoint endpoint) {
        if (! open) {
            throw new IllegalStateException("Service handle was closed.");
        }

        String endpointPath = Util.CN_PATH_PREFIX
                + "/" + Util.coordinateAsPath(coordinate)
                + "/" + Util.CN_ENDPOINTS_NAME
                + "/" + name;

        log.info("Publishing endpoint for " + coordinate.asString() + ": " + endpoint.toJson()
                + " [" + endpointPath + "]"
        );

        try {
            zk.create(endpointPath,
                    endpoint.toJson().getBytes(Util.CHARSET_NAME),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
            throw new CloudnameException.EndpointExists(e);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (UnsupportedEncodingException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
    }

    @Override
    public void removeEndpoint(String name) {
        if (! open) {
            throw new IllegalStateException("Service handle was closed.");
        }

        String endpointPath = Util.CN_PATH_PREFIX
                + "/" + Util.coordinateAsPath(coordinate)
                + "/" + Util.CN_ENDPOINTS_NAME
                + "/" + name;

        try {
            zk.delete(endpointPath, -1);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
    }

    @Override
    public void registerConfigListener(ConfigListener listener) {
        if (! open) {
            throw new IllegalStateException("Service handle was closed.");
        }
    }

    @Override
    public void close() {
        if (! open) {
            throw new IllegalStateException("Service handle was closed.");
        }
        open = false;

        // The nodes that are removed here are ephemeral nodes and
        // we could just let zk remove them, but on the off chance
        // that a single process would try to claim more than one
        // coordinate we provide more explicit cleanup.
        try {
            // Remove endpoints
            for (String s : zk.getChildren(endpointsPath, false)) {
                String endpointPath = endpointsPath + "/" + s;
                zk.delete(endpointPath, -1);
            }

            // Remove status node.  Doing this last is probably
            // the right thing since when we have removed this, we
            // have relinquished ownership of the coordinate.
            log.info("Removing status node " + statusPath);
            zk.delete(statusPath, lastStatusVersion);

        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
    }

    @Override
    public String toString() {
        return coordinate.asString()
                + "[" + prefix + "]"
                ;
    }

    /**
     * Only used by the ZkCloudname class to invalidate
     * ServiceHandles. (Package private).
     */
    public boolean isOpen() {
        return open;
    }
}