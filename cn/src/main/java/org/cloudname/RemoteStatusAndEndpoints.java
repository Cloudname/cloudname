package org.cloudname;

import org.cloudname.zk.Util;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class RemoteStatusAndEndpoints {
    private ServiceStatus serviceStatus = new ServiceStatus(ServiceState.UNASSIGNED,
            "No service state has been assigned");

    private Map<String, Endpoint> endpointsByName = new HashMap<String, Endpoint>();
    private ObjectMapper objectMapper = new ObjectMapper();


    public synchronized ServiceStatus getServiceStatus() {
        return serviceStatus;
    }


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

    public RemoteStatusAndEndpoints(byte[] data) throws IOException {
        serviceStatus = deserialize(new String(data, Util.CHARSET_NAME), new ObjectMapper(), endpointsByName);
    }

    public static ServiceStatus deserialize(
            String data, ObjectMapper objectMapper, Map<String, Endpoint> endpointsByName) throws IOException {

        JsonFactory jsonFactory = new JsonFactory();
        JsonParser jp = jsonFactory.createJsonParser(data);
        String statusString = objectMapper.readValue(jp, new TypeReference<String>() {});
        endpointsByName.clear();
        endpointsByName.putAll((Map<String, Endpoint>)objectMapper.readValue(jp,
                new TypeReference <Map<String, Endpoint>>() {}));
        return ServiceStatus.fromJson(statusString);
    }
}
