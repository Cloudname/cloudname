package org.cloudname.zk;

import org.cloudname.Cloudname;
import org.cloudname.CloudnameException;
import org.cloudname.ConfigListener;
import org.cloudname.Coordinate;
import org.cloudname.Endpoint;
import org.cloudname.Resolver;
import org.cloudname.ServiceHandle;
import org.cloudname.ServiceStatus;
import org.cloudname.ServiceState;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.logging.Logger;

import java.util.concurrent.CountDownLatch;

import java.io.IOException;
import java.io.UnsupportedEncodingException;


/**
 * An implementation of Cloudname using ZooKeeper.
 *
 * This implementation assumes that the path prefix defined by
 * CN_PATH_PREFIX is only used by Cloudname.  The structure and
 * semantics of things under this prefix are defined by this library
 * and will be subject to change.
 *
 * TODO(borud):
 *
 *  - We need a recovery mechanism for when the ZK server we are
 *    connected to goes down.
 *
 *  - when the ZkCloudname instance is close()d the handles should
 *    perhaps be invalidated.
 *
 *  - The exception handling in this class is just atrocious.
 *
 * @author borud
 */
public class ZkCloudname
    implements Cloudname,

{
    private static final Logger log = Logger.getLogger(ZkCloudname.class.getName());

    private static final String CN_STATUS_NAME = "status";
    private static final String CN_ENDPOINTS_NAME = "endpoints";
    private static final String CN_CONFIG_NAME = "config";

    /**
     * A service handle implementation.
     *
     * @author borud
     */
    private class ZkServiceHandle implements ServiceHandle {
        private Coordinate coordinate;
        private volatile boolean open = true;
        private int lastStatusVersion = -1;
        private String prefix;
        private String statusPath;
        private String endpointsPath;
        private String configPath;

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
        public ZkServiceHandle(Coordinate coordinate) {
            this.coordinate = coordinate;

            // Just set some paths for convenience
            prefix = coordinate.asPath();
            statusPath = prefix + "/" + CN_STATUS_NAME;
            endpointsPath = prefix + "/" + CN_ENDPOINTS_NAME;
            configPath = prefix + "/" + CN_CONFIG_NAME;

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

        @Override
        public void putEndpoint(String name, Endpoint endpoint) {
            if (! open) {
                throw new IllegalStateException("Service handle was closed.");
            }

            String endpointPath = CN_PATH_PREFIX
                + "/" + coordinate.asPath()
                + "/" + CN_ENDPOINTS_NAME
                + "/" + name;

            log.info("Publishing endpoint for " + coordinate.asString() + ": " + endpoint.toJson()
                     + " [" + endpointPath + "]"
            );

            try {
                zk.create(endpointPath,
                          endpoint.toJson().getBytes(CHARSET_NAME),
                          Ids.OPEN_ACL_UNSAFE,
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

            String endpointPath = CN_PATH_PREFIX
                + "/" + coordinate.asPath()
                + "/" + CN_ENDPOINTS_NAME
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



    /**
     * Create a given coordinate in the ZooKeeper node tree.
     *
     * Just blindly creates the entire path.  Elements of the path may
     * exist already, but it seems wasteful to
     */
    @Override
    public void createCoordinate(Coordinate coordinate) {
        // Create the root path for the coordinate.  We do this
        // blindly, meaning that if the path already exists, then
        // that's ok -- so a more correct name for this method would
        // be ensureCoordinate(), but that might confuse developers.
        String root = CN_PATH_PREFIX + "/" + coordinate.asPath();
        try {
            ZooKeeperWrapper.mkdir(zk, root, Ids.OPEN_ACL_UNSAFE);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }

        // Create the nodes that represent subdirectories.
        String endpointsPath = root + "/" + CN_ENDPOINTS_NAME;
        String configPath = root + "/" + CN_CONFIG_NAME;
        try {
            log.info("Creating endpoints node " + endpointsPath);
            zk.create(endpointsPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            log.info("Creating config node " + configPath);
            zk.create(configPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
    }

    /**
     * Claim a coordinate.
     *
     * In this implementation a coordinate is claimed by creating an
     * ephemeral with the name defined in CN_STATUS_NAME.  If the node
     * already exists the coordinate has already been claimed.
     */
    @Override
    public ServiceHandle claim(Coordinate coordinate) {
        String path = CN_PATH_PREFIX + "/" + coordinate.asPath() + "/" + CN_STATUS_NAME;
        log.info("Claiming " + coordinate.asString() + " (" + path + ")");

        // Default service status
        ServiceStatus status = new ServiceStatus(ServiceState.UNASSIGNED,
                                                 "No service state has been assigned");
        try {
            zk.create(path,
                      status.toJson().getBytes(CHARSET_NAME),
                      Ids.OPEN_ACL_UNSAFE,
                      CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
            log.info("Coordinate already claimed " + coordinate.asString() + " (" + path + ")");
            throw new CloudnameException.AlreadyClaimed(e);
        } catch (KeeperException.NoNodeException e) {
            log.info("Coordinate does not exist " + coordinate.asString() + " (" + path + ")");
            throw new CloudnameException.CoordinateNotFound(e);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (UnsupportedEncodingException e) {
            // This is not supposed to be happening since CHARSET_NAME
            // should always be "UTF-8".
            throw new CloudnameException(e);
        }

        // If we have come thus far we have succeeded in creating the
        // CN_STATUS_NAME node within the service coordinate directory
        // in ZooKeeper and we can give the client a ServiceHandle.
        return new ZkServiceHandle(coordinate);
    }

    @Override
    public Resolver getResolver() {
        // TODO(borud): implement
        return null;
    }

    @Override
    public ServiceStatus getStatus(Coordinate coordinate) {
        String statusPath = CN_PATH_PREFIX
            + "/" + coordinate.asPath()
            + "/" + CN_STATUS_NAME;

        try {
            Stat stat = new Stat();
            byte[] data = zk.getData(statusPath, null, stat);
            return ServiceStatus.fromJson(new String(data, CHARSET_NAME));
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

