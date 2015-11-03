package org.cloudname.service;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service data stored for each service. This data only contains endpoints at the moment. Endpoint
 * names must be unique.
 *
 * @author stalehd@gmail.com
 */
public class ServiceData {
    private final Object syncObject = new Object();
    private final Map<String, Endpoint> endpoints = new HashMap<>();

    /**
     * Create empty service data object with no endpoints.
     */
    public ServiceData() {

    }

    /**
     * Create a new instance with the given list of endpoints. If there's duplicates in the list
     * the duplicates will be discarded.
     *
     * @param endpointList List of endpoints to add
     */
    /* package-private */ ServiceData(final List<Endpoint> endpointList) {
        synchronized (syncObject) {
            for (final Endpoint endpoint : endpointList) {
                endpoints.put(endpoint.getName(), endpoint);
            }
        }
    }

    /**
     * @param name Name of endpoint
     * @return The endpoint with the specified name. Null if the endpoint doesn't exist
     */
    public Endpoint getEndpoint(final String name) {
        synchronized (syncObject) {
            for (final String epName : endpoints.keySet()) {
                if (epName.equals(name)) {
                    return endpoints.get(name);
                }
            }
        }
        return null;
    }

    /**
     * @param endpoint Endpoint to add
     * @return true if endpoint can be added. False if the endpoint already exists.
     * @throws IllegalArgumentException if endpoint is invalid
     */
    public boolean addEndpoint(final Endpoint endpoint) {
        if (endpoint == null) {
            throw new IllegalArgumentException("Endpoint can not be null");
        }
        synchronized (syncObject) {
            if (endpoints.containsKey(endpoint.getName())) {
                return false;
            }
            endpoints.put(endpoint.getName(), endpoint);
        }
        return true;
    }

    /**
     * @param endpoint endpoint to remove
     * @return True if the endpoint has been removed, false if the endpoint can't be removed. Nulls
     * @throws IllegalArgumentException if endpoint is invalid
     */
    public boolean removeEndpoint(final Endpoint endpoint) {
        if (endpoint == null) {
            throw new IllegalArgumentException("Endpoint can't be null");
        }
        synchronized (syncObject) {
            if (!endpoints.containsKey(endpoint.getName())) {
                return false;
            }
            endpoints.remove(endpoint.getName());
        }
        return true;
    }

    /**
     * @return Service data serialized as a JSON string
     */
    /* package-private */ String toJsonString() {
        final JSONArray epList = new JSONArray();
        int i = 0;
        for (Map.Entry<String, Endpoint> entry : endpoints.entrySet()) {
            epList.put(i++, new JSONObject(entry.getValue().toJsonString()));
        }
        return new JSONObject().put("endpoints", epList).toString();
    }

    /**
     * @param jsonString JSON string to create instance from
     * @throws IllegalArgumentException if parameter is invalid
     */
    /* package-private */ static ServiceData fromJsonString(final String jsonString) {
        if (jsonString == null || jsonString.isEmpty()) {
            throw new IllegalArgumentException("json string can not be null or empty");
        }

        final List<Endpoint> endpoints = new ArrayList<>();

        final JSONObject json = new JSONObject(jsonString);
        final JSONArray epList = json.getJSONArray("endpoints");
        for (int i = 0; i < epList.length(); i++) {
            endpoints.add(Endpoint.fromJson(epList.getJSONObject(i).toString()));
        }
        return new ServiceData(endpoints);
    }
}
