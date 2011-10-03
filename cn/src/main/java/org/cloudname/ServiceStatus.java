package org.cloudname;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;

/**
 * A representation of the basic runtime status of a service.
 *
 * Instances of ServiceStatus are immutable.
 *
 * @author borud
 */
public class ServiceStatus {
    private final ServiceState state;
    private final String message;

    /**
     * @param state the state of the service
     * @param message a human readable message
     */
    @JsonCreator
    public ServiceStatus(@JsonProperty("state") ServiceState state,
                         @JsonProperty("message") String message)
    {
        this.state = state;
        this.message = message;
    }

    public ServiceState getState() {
        return state;
    }

    public String getMessage() {
        return message;
    }

    public static ServiceStatus fromJson(String json) throws IOException {
        return new ObjectMapper().readValue(json, ServiceStatus.class);
    }

    public String toJson() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (IOException e) {
            return null;
        }
    }
}