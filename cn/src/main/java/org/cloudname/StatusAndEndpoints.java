package org.cloudname;

import org.cloudname.zk.Util;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  StatusAndEndpoints represent the data about a coordinate. It is immutable as it represent a state.
 *  There is an inner public class that is responsible for building.
 *  The class has support for deserializing and serializing the data and methods for accessing endpoints.
 *
 *  @auther dybdahl
 */
public class StatusAndEndpoints {

    /**
     * The status of the coordinate, is it running etc.
     */
    final private ServiceStatus serviceStatus;

    /**
     * The endpoints registered at the coordinate mapped by endpoint name.
     */
    final private Map<String, Endpoint> endpointsByName;

    /**
     * Getter for status of coordiante.
     * @return the service status of the coordinate.
     */
    public synchronized ServiceStatus getServiceStatus() {
        return serviceStatus;
    }

    /**
     * Getter for endpoint of the coordinate given the endpoint name.
     * @param name of the endpoint.
     * @return the endpoint or null if non-existing.
     */
    public synchronized Endpoint getEndpoint(String name) {
        return endpointsByName.get(name);
    }

    /**
     * Return all endpoints.
     * @param endpoints The endpoints are put in this list.
     */
    public synchronized void returnAllEndpoints(List<Endpoint> endpoints) {
        for (Endpoint endpoint : endpointsByName.values()) {
            endpoints.add(endpoint);
        }
    }

    /**
     * Return a serialized string representing the status and endpoint. It can be used by the builder to de-serialize.
     * @return The serialized string.
     * @throws IOException if something goes wrong, should not be a common problem though.
     */
    public synchronized String serialize()  throws IOException {
        StringWriter stringWriter = new StringWriter();
        JsonGenerator generator;

        generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(stringWriter);

        generator.writeString(serviceStatus.toJson());
        generator.writeObject(endpointsByName);


        generator.flush();

        return new String(stringWriter.getBuffer());
    }

    /**
     * Private constructor, use the Builder to create an instance.
     */
    private StatusAndEndpoints(ServiceStatus serviceStatus, Map<String, Endpoint> endpointsByName) {
        this.serviceStatus = serviceStatus;
        this.endpointsByName = endpointsByName;
    }

    /**
     * Class for building StatusAndEndpoints. It is ok to call build() multiple times and modify the builder
     * after build, however this will of course not modified the previous built objects.
     */
    static public class Builder {
        private ObjectMapper objectMapper = new ObjectMapper();
        private ServiceStatus serviceStatus = new ServiceStatus(ServiceState.UNASSIGNED,
                "No service state has been assigned");
        private Map<String, Endpoint> endpointsByName = new HashMap<String, Endpoint>();

        /**
         * Build a new immutable StatusAndEndpoints object.
         */
        public StatusAndEndpoints build() {
            return new StatusAndEndpoints(serviceStatus, endpointsByName);
        }

        /**
         * Updates status, overwrite any existing status information.
         */
        public synchronized Builder updateStatus(ServiceStatus status)  {
            this.serviceStatus = status;
            return this;
        }

        /**
         * Adds new endpoints to the builder.
         * @throws EndpointException if any of the endpoints already exists.
         */
        public synchronized Builder putEndpoints(List<Endpoint> newEndpoints) throws EndpointException {
            for (Endpoint endpoint : newEndpoints) {
                if (endpointsByName.containsKey(endpoint.getName())) {
                    throw new EndpointException("endpoint already exists: " +  endpoint.getName());
                }
                endpointsByName.put(endpoint.getName(), endpoint);
            }
            return this;
        }

        /**
         * Remove endpoints from the Builder object.
         * @throws EndpointException if any endpoints does not exist.
         */
        public synchronized Builder removeEndpoints(List<String> names) throws EndpointException {
            for (String name : names) {
                if (! endpointsByName.containsKey(name)) {
                    throw new EndpointException("endpoint does not exist: " +  name);
                }
                if (null == endpointsByName.remove(name)) {
                    throw new EndpointException("End point does not exists.");
                }
            }
            return this;
        }

        /**
         * Sets the state of the Builder object based on a serialized byte string.
         * Any old data is overwritten.
         * @throws IOException if something went wrong, should not happen on valid data.
         */
        public synchronized Builder deserialize(byte[] data) throws IOException {
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
