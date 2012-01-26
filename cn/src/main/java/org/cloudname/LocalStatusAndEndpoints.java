package org.cloudname;

import org.cloudname.zk.ZkStatusAndEndpoints;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class LocalStatusAndEndpoints {

    private ServiceStatus serviceStatus = new ServiceStatus(ServiceState.UNASSIGNED,
            "No service state has been assigned");

    private Map<String, Endpoint> endpointsByName = new HashMap<String, Endpoint>();

    public synchronized void updateStatus(ServiceStatus status)  {
        this.serviceStatus = status;
    }


    public synchronized void putEndpoints(List<Endpoint> newEndpoints) throws EndpointException {
        for (Endpoint endpoint : newEndpoints) {
            if (endpointsByName.containsKey(endpoint.getName())) {
                throw new EndpointException("endpoint already exists: " +  endpoint.getName());
            }
            endpointsByName.put(endpoint.getName(), endpoint);
        }
    }

    public synchronized void removeEndpoints(List<String> names)
            throws EndpointException, CloudnameException, CoordinateMissingException {
        for (String name : names) {
            if (! endpointsByName.containsKey(name)) {
                throw new EndpointException("endpoint does not exist: " +  name);
            }
            if (null == endpointsByName.remove(name)) {
                throw new EndpointException("End point does not exists.");
            }
        }
    }

    private static String serialize(ServiceStatus status, Map<String, Endpoint> endpointsByName)
            throws CloudnameException, IOException {
        StringWriter stringWriter = new StringWriter();
        JsonGenerator generator;

        generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(stringWriter);

        try {
            generator.writeString(status.toJson());
            generator.writeObject(endpointsByName);
        } catch (IOException e) {
            throw new CloudnameException(e);
        }

        generator.flush();

        return new String(stringWriter.getBuffer());
    }
}
