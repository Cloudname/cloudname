package org.cloudname.service;

import org.cloudname.core.CloudnamePath;
import org.json.JSONObject;

/**
 * Endpoints exposed by services. Endpoints contains host address and port number.
 *
 * @author stalehd@gmail.com
 */
public class Endpoint {
    private final String name;
    private final String host;
    private final int port;

    /**
     * @param name Name of endpoint. Must conform to RFC 952 and RFC 1123,
     *     ie [a-z,0-9,-]
     * @param host Host name or IP address
     * @param port Port number (1- max port number)
     * @throws IllegalArgumentException if one of the parameters are null (name/host) or zero (port)
     */
    public Endpoint(final String name, final String host, final int port) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Name can not be null or empty");
        }
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Host can not be null or empty");
        }
        if (port < 1) {
            throw new IllegalArgumentException("Port can not be < 1");
        }
        if (!CloudnamePath.isValidPathElementName(name)) {
            throw new IllegalArgumentException("Name is not a valid identifier");
        }

        this.name = name;
        this.host = host;
        this.port = port;
    }

    /**
     * The endpoint's name.
     */
    public String getName() {
        return name;
    }

    /**
     * The endpoint's host name or IP address.
     */
    public String getHost() {
        return host;
    }

    /**
     * The endpoint's port number.
     */
    public int getPort() {
        return port;
    }

    /**
     * JSON representation of endpoint.
     */
    /* package-private */ String toJsonString() {
        return new JSONObject()
                .put("name", name)
                .put("host", host)
                .put("port", port)
                .toString();
    }

    /**
     * Create new Endpoint instance from JSON string
     *
     * @throws org.json.JSONException if the string is malformed.
     */
    /* package-private */ static Endpoint fromJson(final String jsonString) {
        final JSONObject json = new JSONObject(jsonString);
        return new Endpoint(
                json.getString("name"),
                json.getString("host"),
                json.getInt("port"));
    }

    @Override
    public boolean equals(final Object otherInstance) {
        if (otherInstance == null || !(otherInstance instanceof Endpoint)) {
            return false;
        }
        final Endpoint other = (Endpoint) otherInstance;

        if (!this.name.equals(other.name)
                || !this.host.equals(other.host)
                || this.port != other.port) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public String toString() {
        return "[ name = " + name
                + ", host = " + host
                + ", port = " + port
                + "]";
    }
}
