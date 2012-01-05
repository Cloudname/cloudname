package org.cloudname;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;

/**
 * Representation of an endpoint.  This class is used to describe a
 * wide range of endpoints, but it is, initially geared mainly towards
 * services for which we need to know a hostname, port and protocol.
 * As a stop-gap measure we provide an {@code endpointData} field
 * which can be used in a pinch to communicate extra information about
 * the endpoint.
 *
 * Instances of this class are immutable.
 *
 * TODO(borud): decide if coordinateFlag and name should be part of this
 *   class.
 *
 * @author borud
 */
public class Endpoint {
    // This gets saved into ZooKeeper as well and is redundant info,
    // but it makes sense to have this information in the Endpoint
    // instances to make it possible for clients to get a list of
    // endpoints and be able to figure out what coordinates they come
    // from if they were gathered from multiple services.
    private final Coordinate coordinate;
    // Ditto for name.
    private final String name;
    private final String host;
    private final int port;
    private final String protocol;
    private final String endpointData;

    @JsonCreator
    public Endpoint(@JsonProperty("coordinate") Coordinate coordinate,
                    @JsonProperty("name") String name,
                    @JsonProperty("host") String host,
                    @JsonProperty("port") int port,
                    @JsonProperty("protocol") String protocol,
                    @JsonProperty("endpointData") String endpointData)
    {
        this.coordinate = coordinate;
        this.name = name;
        this.host = host;
        this.port = port;
        this.protocol = protocol;
        this.endpointData = endpointData;
    }

    public Coordinate getCoordinate() {
        return coordinate;
    }

    public String getName() {
        return name;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getProtocol() {
        return protocol;
    }

    public String getEndpointData() {
        return endpointData;
    }

    public static Endpoint fromJson(String json) throws IOException {
        return new ObjectMapper().readValue(json, Endpoint.class);
    }

    public String toJson() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (IOException e) {
            return null;
        }
    }
}