package org.cloudname.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.cloudname.CloudnameException;
import org.cloudname.Endpoint;
import org.cloudname.ServiceState;
import org.cloudname.ServiceStatus;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *  ZkCoordinateData represent the data regarding a coordinate. It can return an immutable snapshot.
 *  The class has support for deserializing and serializing the data and methods for accessing
 *  endpoints. The class is fully thread-safe.
 *
 *  @auther dybdahl
 */
public final class ZkCoordinateData {
    /**
     * The status of the coordinate, is it running etc.
     */
    private ServiceStatus serviceStatus = new ServiceStatus(ServiceState.UNASSIGNED,
            "No service state has been assigned");

    /**
     * The endpoints registered at the coordinate mapped by endpoint name.
     */
    private final Map<String, Endpoint> endpointsByName = new HashMap<String, Endpoint>();

    // Used for deserializing.
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Object localVariablesMonitor = new Object();

    /**
     * Create a new immutable snapshot object.
     */
    public Snapshot snapshot() {
        synchronized (localVariablesMonitor) {
            return new Snapshot(serviceStatus, endpointsByName);
        }
    }

    /**
     * Sets status, overwrite any existing status information.
     */
    public ZkCoordinateData setStatus(ServiceStatus status)  {
        synchronized (localVariablesMonitor) {
            this.serviceStatus = status;
            return this;
        }
    }

    /**
     * Adds new endpoints to the builder. It is not legal to add a new endpoint with an endpoint
     * that already exists.
     */
    public ZkCoordinateData putEndpoints(final List<Endpoint> newEndpoints) {
        synchronized (localVariablesMonitor) {
            for (Endpoint endpoint : newEndpoints) {
                Endpoint previousEndpoint = endpointsByName.put(endpoint.getName(), endpoint);
                // Calling put on an existing endpoint is not allowed.
                if (null != previousEndpoint) {
                    throw new IllegalArgumentException("endpoint already exists: "
                            +  endpoint.getName());
                }
            }
        }
        return this;
    }

    /**
     * Remove endpoints from the Dynamic object.
     */
    public ZkCoordinateData removeEndpoints(final List<String> names)  {
        synchronized (localVariablesMonitor) {
            for (String name : names) {
                if (! endpointsByName.containsKey(name)) {
                    throw new IllegalArgumentException("endpoint does not exist: " +  name);
                }
                if (null == endpointsByName.remove(name)) {
                    throw new IllegalArgumentException(
                            "Endpoint does not exists, null in internal structure." + name);
                }
            }
        }
        return this;
    }

    /**
     * Sets the state of the Dynamic object based on a serialized byte string.
     * Any old data is overwritten.
     * @throws IOException if something went wrong, should not happen on valid data.
     */
    public ZkCoordinateData deserialize(byte[] data) throws IOException {
        synchronized (localVariablesMonitor) {
            final String stringData = new String(data, Util.CHARSET_NAME);
            final JsonFactory jsonFactory = new JsonFactory();
            final JsonParser jp = jsonFactory.createJsonParser(stringData);
            final String statusString = objectMapper.readValue(jp, new TypeReference<String>() {});
            serviceStatus = ServiceStatus.fromJson(statusString);
            endpointsByName.clear();
            endpointsByName.putAll((Map<String, Endpoint>)objectMapper.readValue(jp,
                    new TypeReference <Map<String, Endpoint>>() {}));
        }
        return this;
    }

    /**
     * An immutable representation of the coordinate data.
     */
    public static class Snapshot {
        /**
         * The status of the coordinate, is it running etc.
         */
        private final ServiceStatus serviceStatus;

        /**
         * The endpoints registered at the coordinate mapped by endpoint name.
         */
        private final Map<String, Endpoint> endpointsByName;

        /**
         * Getter for status of coordinate.
         * @return the service status of the coordinate.
         */
        public ServiceStatus getServiceStatus() {
            return serviceStatus;
        }

        /**
         * Getter for endpoint of the coordinate given the endpoint name.
         * @param name of the endpoint.
         * @return the endpoint or null if non-existing.
         */
        public Endpoint getEndpoint(final String name) {
            return endpointsByName.get(name);
        }

        /**
         * Returns all the endpoints.
         * @return set of endpoints.
         */
        public Set<Endpoint> getEndpoints() {
            Set<Endpoint> endpoints = new HashSet<Endpoint>();
            endpoints.addAll(endpointsByName.values());
            return endpoints;
        }

        /**
         * A method for getting all endpoints.
         * @param endpoints The endpoints are put in this list.
         */
        public void appendAllEndpoints(final Collection<Endpoint> endpoints) {
            endpoints.addAll(endpointsByName.values());
        }

        /**
         * Return a serialized string representing the status and endpoint. It can be de-serialize
         * by the inner class.
         * @return The serialized string.
         * @throws IOException if something goes wrong, should not be a common problem though.
         */
        public String serialize() {
            final StringWriter stringWriter = new StringWriter();
            final JsonGenerator generator;

            try {
                generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(stringWriter);
                generator.writeString(serviceStatus.toJson());
                generator.writeObject(endpointsByName);

                generator.flush();
            } catch (IOException e) {
                throw new RuntimeException(
                        "Got IOException while serializing coordinate data." , e);
            }
            return new String(stringWriter.getBuffer());
        }

        /**
         * Private constructor, only ZkCoordinateData can build this.
         */
        private Snapshot(ServiceStatus serviceStatus, Map<String, Endpoint> endpointsByName) {
            this.serviceStatus = serviceStatus;
            this.endpointsByName = endpointsByName;
        }
    }

    /**
     * Utility function to create and load a ZkCoordinateData from ZooKeeper.
     * @param watcher for callbacks from ZooKeeper. It is ok to pass null.
     * @throws CloudnameException when problems loading data.
     */
    static public ZkCoordinateData loadCoordinateData(
            final String statusPath, final ZooKeeper zk, final Watcher watcher)
            throws CloudnameException {
        Stat stat = new Stat();
        try {
            byte[] data;
            if (watcher == null) {
                data = zk.getData(statusPath, false, stat);
            } else {
                data = zk.getData(statusPath, watcher, stat);
            }
            return new ZkCoordinateData().deserialize(data);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (UnsupportedEncodingException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (IOException e) {
            throw new CloudnameException(e);
        }
    }
}
