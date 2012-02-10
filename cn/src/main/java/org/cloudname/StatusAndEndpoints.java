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


public class StatusAndEndpoints {
    final private ServiceStatus serviceStatus;
    final private Map<String, Endpoint> endpointsByName;
   
    public synchronized ServiceStatus getServiceStatus() {
        return serviceStatus;
    }

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
    
    public synchronized String serialize()  throws CloudnameException, IOException {
        StringWriter stringWriter = new StringWriter();
        JsonGenerator generator;

        generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(stringWriter);

        try {
            generator.writeString(serviceStatus.toJson());
            generator.writeObject(endpointsByName);
        } catch (IOException e) {
            throw new CloudnameException(e);
        }

        generator.flush();

        return new String(stringWriter.getBuffer());
    }

    private StatusAndEndpoints(ServiceStatus serviceStatus, Map<String, Endpoint> endpointsByName) {
        this.serviceStatus = serviceStatus;
        this.endpointsByName = endpointsByName;
    }

    static public class Builder {
        private ObjectMapper objectMapper = new ObjectMapper();
        private ServiceStatus serviceStatus = new ServiceStatus(ServiceState.UNASSIGNED,
                "No service state has been assigned");
        private Map<String, Endpoint> endpointsByName = new HashMap<String, Endpoint>();

        public StatusAndEndpoints build() {
            return new StatusAndEndpoints(serviceStatus, endpointsByName);
        }

        public synchronized Builder updateStatus(ServiceStatus status)  {
            this.serviceStatus = status;
            return this;
        }

        public synchronized Builder putEndpoints(List<Endpoint> newEndpoints) throws EndpointException {
            for (Endpoint endpoint : newEndpoints) {
                if (endpointsByName.containsKey(endpoint.getName())) {
                    throw new EndpointException("endpoint already exists: " +  endpoint.getName());
                }
                endpointsByName.put(endpoint.getName(), endpoint);
            }
            return this;
        }

        public synchronized Builder removeEndpoints(List<String> names)
                throws EndpointException, CloudnameException, CoordinateMissingException {
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
