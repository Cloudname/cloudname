package org.cloudname.zk;

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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  CoordinateDataSnapshot represent the data about a coordinate. It is immutable as it represent a state.
 *  There is an inner public class that is responsible for building.
 *  The class has support for deserializing and serializing the data and methods for accessing endpoints.
 *
 *  @auther dybdahl
 */
public class CoordinateDataSnapshot {

    /**
     * The status of the coordinate, is it running etc.
     */
    private final ServiceStatus serviceStatus;

    /**
     * The endpoints registered at the coordinate mapped by endpoint name.
     */
    private final Map<String, Endpoint> endpointsByName;

    /**
     * Getter for status of coordiante.
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
    public Endpoint getEndpoint(String name) {
        return endpointsByName.get(name);
    }

    /**
     * Return all endpoints.
     * @param endpoints The endpoints are put in this list.
     */
    public void appendAllEndpoints(Collection<Endpoint> endpoints) {
        endpoints.addAll(endpointsByName.values());
    }

    /**
     * Return a serialized string representing the status and endpoint. It can be used by the builder to de-serialize.
     * @return The serialized string.
     * @throws IOException if something goes wrong, should not be a common problem though.
     */
    public String serialize()  throws IOException {
        StringWriter stringWriter = new StringWriter();
        JsonGenerator generator;

        generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(stringWriter);

        generator.writeString(serviceStatus.toJson());
        generator.writeObject(endpointsByName);


        generator.flush();

        return new String(stringWriter.getBuffer());
    }

    /**
     * Private constructor, use the Dynamic to create an instance.
     */
    private CoordinateDataSnapshot(ServiceStatus serviceStatus, Map<String, Endpoint> endpointsByName) {
        this.serviceStatus = serviceStatus;
        this.endpointsByName = endpointsByName;
    }

    /**
     * Class for building CoordinateDataSnapshot. It is ok to call snapshot() multiple times and modify the builder
     * after snapshot, however this will of course not modified the previous built objects.
     */
    static public class Dynamic {
        private ObjectMapper objectMapper = new ObjectMapper();
        private ServiceStatus serviceStatus = new ServiceStatus(ServiceState.UNASSIGNED,
                "No service state has been assigned");
        private Map<String, Endpoint> endpointsByName = new HashMap<String, Endpoint>();

        /**
         * Create a new immutable CoordinateDataSnapshot object.
         */
        public CoordinateDataSnapshot snapshot() {
            return new CoordinateDataSnapshot(serviceStatus, endpointsByName);
        }

        /**
         * Sets status, overwrite any existing status information.
         */
        public Dynamic setStatus(ServiceStatus status)  {
            this.serviceStatus = status;
            return this;
        }

        /**
         * Adds new endpoints to the builder. It is not legal to add a new endpoint with an endpoint that already
         * exists.
         */
        public Dynamic putEndpoints(List<Endpoint> newEndpoints) {
            for (Endpoint endpoint : newEndpoints) {
                if (null != endpointsByName.put(endpoint.getName(), endpoint)) {
                    throw new IllegalArgumentException("endpoint already exists: " +  endpoint.getName());
                }
            }
            return this;
        }

        /**
         * Remove endpoints from the Dynamic object.
         */
        public Dynamic removeEndpoints(List<String> names)  {
            for (String name : names) {
                if (! endpointsByName.containsKey(name)) {
                    throw new IllegalArgumentException("endpoint does not exist: " +  name);
                }
                if (null == endpointsByName.remove(name)) {
                    throw new IllegalArgumentException("Endpoint does not exists.");
                }
            }
            return this;
        }

        /**
         * Sets the state of the Dynamic object based on a serialized byte string.
         * Any old data is overwritten.
         * @throws IOException if something went wrong, should not happen on valid data.
         */
        public Dynamic deserialize(byte[] data) throws IOException {
            String stringData = new String(data, Util.CHARSET_NAME);
            JsonFactory jsonFactory = new JsonFactory();
            JsonParser jp = jsonFactory.createJsonParser(data);
            String statusString = objectMapper.readValue(jp, new TypeReference<String>() {});
            serviceStatus = ServiceStatus.fromJson(statusString);
            endpointsByName.clear();
            endpointsByName.putAll((Map<String, Endpoint>)objectMapper.readValue(jp,
                    new TypeReference <Map<String, Endpoint>>() {}));
            return this;
        }
    }
}
