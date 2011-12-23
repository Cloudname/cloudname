package org.cloudname.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.cloudname.*;
import org.codehaus.jackson.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * This class keeps track of serviceStatus and endpoints for a coordinate.
 * It has an inner class for building an instance (Builder).
 *
 * TODO(dybdahl): Add support for claiming an existing node. This could be used for
 * recovery after network failure etc.
 *
 * @author dybdahl
 */
public class ZkStatusAndEndpoints {
    
    enum State {
        /**
         * We don't know the state.
         */
        UNKNOWN,
        /**
         * We have loaded the state from ZooKeeper. It means that we are in a read-only mode.
         */
        LOADED,
        /**
         * We have claimed and own the coordinate. We are the only one that are allowed to modify.
         */
        CLAIMED
    }

    private static final Logger log = Logger.getLogger(ZkStatusAndEndpoints.class.getName());

    private State state;
    private final ZooKeeper zk;
    private final String path;
    private int lastStatusVersion;
    private final ObjectMapper objectMapper;
    private ServiceStatus serviceStatus;
    private final Map<String, Endpoint> endpointsByName;

    /**
     * We keep this private, the correct way to build an instance is to use the Builder.
     * @param builder Contains the data for the instance.
     */
    private ZkStatusAndEndpoints(Builder builder) {
        this.zk = builder.getZooKeeper();
        this.path = builder.getPath();
        this.objectMapper = builder.getObjectMapper();
        this.lastStatusVersion = builder.getLastStatusVersion();
        this.state = builder.getState();
        this.endpointsByName = builder.getEndpointsByName();
        this.serviceStatus = builder.getServiceStatus();
    }

    /**
     * Updates the ServiceStatus and persists it. Only allowed if we claimed the coordinate.
     * @param status The new value for serviceStatus.
     */
    public synchronized void updateStatus(ServiceStatus status) {
        if (state != State.CLAIMED) {
            throw new IllegalStateException("This instance did not claim this coordinate.");
        }
        this.serviceStatus = status;
        writeStatusEndpoint();
    }

    /**
     * Returns the ServiceStatus. If we claimed the coordinate, this is the real-time value, otherwise
     * it is the last loaded value.
     * @return ServiceStatus.
     */
    public synchronized ServiceStatus getServiceStatus() {
        return serviceStatus;
    }

    /**
     * Returns the Endpoint. If we claimed the coordinate, this is the real-time value, otherwise
     * it is the last loaded value.
     * @return Endpoint.
     */
    public synchronized Endpoint getEndpoint(String name) {
        return endpointsByName.get(name);
    }

    /**
     * Set all endpoints to the endpoints argument.
     * @param endpoints The endpoints are put in this list.
     */
    public synchronized void returnAllEndpoints(List<Endpoint> endpoints) {
        for (Endpoint endpoint : endpointsByName.values()) {
            endpoints.add(endpoint);
        }
    }

    /**
     * Adds new endpoints and persist them. Requires that this instance owns the claim to the coordinate.
     * @param newEndpoints endpoints to be added.
     */
    public synchronized void putEndpoints(List<Endpoint> newEndpoints) {
        if (state != State.CLAIMED) {
            throw new IllegalStateException("This instance did not claim this coordinate.");
        }
        for (Endpoint endpoint : newEndpoints) {
            if (endpointsByName.containsKey(endpoint.getName())) {
                log.info("endpoint already exists: " +  endpoint.getName());
                throw new CloudnameException.EndpointExists();
            }
            endpointsByName.put(endpoint.getName(), endpoint);
        }
        writeStatusEndpoint();
    }

    /**
     * Remove endpoints and persist it. Requires that this instance owns the claim to the coordinate.
     * @param names names of endpoints to be removed.
     */
    public synchronized void removeEndpoints(List<String> names) {
        if (state != State.CLAIMED) {
            throw new IllegalStateException("This instance did not claim this coordinate.");
        }
        for (String name : names) {
            if (! endpointsByName.containsKey(name)) {
                log.info("endpoint does not exist: " +  name);
                throw new CloudnameException.EndpointDoesNotExist();
            }
            endpointsByName.remove(name);
        }
        writeStatusEndpoint();
    }

    /**
     * Release the claim of the coordinate. It means that nobody owns the coordinate anymore.
     * Requires that that this instance owns the claim to the coordinate.
     */
    public synchronized void releaseClaim() {
        if (state != State.CLAIMED) {
            throw new IllegalStateException("This instance did not own the claim to this coordinate.");
        }
        // The nodes that are removed here are ephemeral nodes and
        // we could just let zk remove them, but on the off chance
        // that a single process would try to claim more than one
        // coordinate we provide more explicit cleanup.
        try {
            zk.delete(path, lastStatusVersion);

        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
        serviceStatus = new ServiceStatus(ServiceState.UNASSIGNED,
                "No service state has been assigned");
        state = State.LOADED;
        endpointsByName.clear();
        lastStatusVersion = -1;
    }

    /**
     * Creates a string for debugging etc
     * @return serialized version of the instance data.
     */
    public synchronized String toString() {
       return serialize(serviceStatus, endpointsByName);
    }

    /**
     * Creates a json string based on serviceStatus and endpoints.
     * @see ZkStatusAndEndpoints#deserialize for de-serialization.
     * @param status
     * @param endpointsByName
     * @return The serialized string.
     */
    private static String serialize(ServiceStatus status, Map<String, Endpoint> endpointsByName) {
        StringWriter stringWriter = new StringWriter();
        JsonGenerator generator;
        try {
           generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(stringWriter);
        } catch (IOException e) {
            throw new CloudnameException(e);

        }
        try {
            generator.writeString(status.toJson());
            generator.writeObject(endpointsByName);
        } catch (IOException e) {
            throw new CloudnameException(e);

        }
        try {
            generator.flush();
        } catch (IOException e) {
            throw new CloudnameException(e);
        }
        return new String(stringWriter.getBuffer());
    }

    /**
     * Deserialize a json string into the endpointsByName and the return value.
     * @see ZkStatusAndEndpoints#serialize for de-serialization.
     * @param data The string to be deserialized.
     * @param objectMapper A helper object.
     * @param endpointsByName This map is populated with the endpoints.
     * @return the ServiceStatus in the data.                                             ´
     */
    private static ServiceStatus deserialize(String data, ObjectMapper objectMapper, Map<String, Endpoint> endpointsByName) {

        JsonFactory jsonFactory = new JsonFactory();
        try {

            JsonParser jp = jsonFactory.createJsonParser(data);
            String statusString = objectMapper.readValue(jp, new TypeReference <String>() {});
            endpointsByName.clear();
            endpointsByName.putAll((Map<String, Endpoint>)objectMapper.readValue(jp, new TypeReference <Map<String, Endpoint>>() {}));
            return ServiceStatus.fromJson(statusString);

        } catch (IOException e) {
            throw new CloudnameException(e);

        }
    }

    /**
     * Creates the serialized value of the object and stores this in ZooKeeper under the path.
     * It updates the lastStatusVersion.
     */
    private void writeStatusEndpoint() {
        if (state != State.CLAIMED) {
            throw new IllegalStateException("This instance did not claim this coordinate.");
        }
        try {

            Stat stat = zk.setData(path,
                        serialize(serviceStatus, endpointsByName).getBytes(Util.CHARSET_NAME),
                        lastStatusVersion);
             lastStatusVersion = stat.getVersion();
        } catch (KeeperException.NodeExistsException e) {
            log.info("Coordinate already claimed (" + path + ")");
            throw new CloudnameException.AlreadyClaimed(e);
        } catch (KeeperException.NoNodeException e) {
            log.info("Coordinate does not exist (" + path + ")");
            throw new CloudnameException.CoordinateNotFound(e);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (UnsupportedEncodingException e) {
            throw new CloudnameException(e);
        }
    }

    /**
     * This class can build ZkStatusAndEndpoints.
     * If you want to create an instance that first claims the coordinate it can be done like this:
     *   ZkStatusAndEndpoints statusAndEndpoints = new ZkStatusAndEndpoints.Builder(zk, statusPath).claim().build();
     * Instead of claim you can also load the coordinate.
     */
    public static class Builder {
        private ZkStatusAndEndpoints.State state = ZkStatusAndEndpoints.State.UNKNOWN;
        private ZooKeeper zk = null;
        private String path = null;
        private int lastStatusVersion = -1000;
        private ServiceStatus serviceStatus = new ServiceStatus(ServiceState.UNASSIGNED,
                "No service state has been assigned");
        private Map<String, Endpoint> endpointsByName = new HashMap<String, Endpoint>();
        private ObjectMapper objectMapper = new ObjectMapper();

        /**
         * Constructor for builder.
         * @param zk The ZooKeeper instance to use.
         * @param path The serviceStatus/endpoints path of the coordinate to claim or load.
         */
        Builder(ZooKeeper zk, String path) {
            this.zk = zk;
            this.path = path;
        }

        /**
         * Returns the ZooKeeper.
         * @return ZooKeeper.
         */
        public ZooKeeper getZooKeeper() {
            return zk;
        }

        /**
         * Returns the path.
         * @return serviceStatus/endpoints path of the coordinate.
         */
        public String getPath() {
            return path;
        }

        /**
         * Returns the serviceStatus.
         * @return serviceStatus.
         */
        public ServiceStatus getServiceStatus() {
            return serviceStatus;
        }

        /**
         * Returns the ObjectMapper instance.
         * @return ObjectMapper.
         */
        public ObjectMapper getObjectMapper() {
            return objectMapper;
        }

        /**
         * Returns the State.
         * @return state
         */
        public ZkStatusAndEndpoints.State getState() {
            return state;
        }

        /**
         * Returns the version if the coordinate was claimed.
         * @return version.
         */
        public int getLastStatusVersion() {
            return lastStatusVersion;
        }

        /**
         * Returns a list of Endpoints if loaded or an empty list of endpoints if claimed.
         * @return Map<String, Endpoint> endpoints.
         */
        public Map<String, Endpoint> getEndpointsByName() {
            return endpointsByName;
        }

        /**
         * Builds an instance of ZkStatusAndEndpoints. Requires that load() or claim() is called upfront.
         * @return this.
         */
        public ZkStatusAndEndpoints build() {
            if (state == ZkStatusAndEndpoints.State.UNKNOWN) {
                throw new IllegalStateException("Call load or claim before building.");
            }
            return new ZkStatusAndEndpoints(this);
        }

        /**
         * Loads the coordinate from ZooKeeper.
         * @return this.
         */
        public Builder load() {
            if (state != ZkStatusAndEndpoints.State.UNKNOWN) {
                throw new IllegalStateException("Does not make sense to load when something is already set.");
            }
            Stat stat = new Stat();
            try {
                byte[] data = zk.getData(path, false /*watcher*/, stat);

                serviceStatus = deserialize(new String(data, Util.CHARSET_NAME), objectMapper, endpointsByName);


            } catch (KeeperException e) {
                throw new CloudnameException(e);

            } catch (InterruptedException e) {
                throw new CloudnameException(e);

            } catch (UnsupportedEncodingException e) {
                throw new CloudnameException(e);
            }
            state = State.LOADED;

            return this;
        }

        /**
         * Claims a coordinate.
         * @return this.
         */
        public Builder claim() {
            if (state != State.UNKNOWN) {
                throw new IllegalStateException("Does not make sense to claim when something is already loaded.");
            }
            try {
                zk.create(
                        path, serialize(serviceStatus, endpointsByName).getBytes(Util.CHARSET_NAME), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException.NodeExistsException e) {
                log.info("Coordinate already claimed  (" + path + ")");
                throw new CloudnameException.AlreadyClaimed(e);
            } catch (KeeperException.NoNodeException e) {
                log.info("Coordinate does not exist  (" + path + ")");
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

            // Stat the serviceStatus node so we have the version.  If later
            // we try to operate on the serviceStatus node and we do not have
            // the correct version this can mean that someone else has
            // been meddling with the serviceStatus node.  In which case we
            // must complain loudly.
            try {
                Stat stat = zk.exists(path, false);
                lastStatusVersion = stat.getVersion();
            } catch (KeeperException e) {
                throw new CloudnameException(e);
            } catch (InterruptedException e) {
                throw new CloudnameException(e);
            }
            state = State.CLAIMED;
            return this;
        }
    }
}