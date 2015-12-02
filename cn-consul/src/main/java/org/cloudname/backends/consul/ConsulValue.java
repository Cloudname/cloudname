package org.cloudname.backends.consul;

import org.glassfish.jersey.internal.util.Base64;
import org.json.JSONObject;

/**
 * A simple representation of the values in the KV store. Each value has a key, a value and a
 * modify index. The modify index is updated by Consul when the value is changed.
 *
 * @author Ståle Dahl <stalehd@gmail.com>
 */
public class ConsulValue {
    private final int modifyIndex;
    private final String key;
    private final String value;

    /**
     * @param modifyIndex The modify index of the value
     * @param key Name of the value
     * @param value The value itself
     */
    public ConsulValue(final int modifyIndex, final String key, final String value) {
        this.modifyIndex = modifyIndex;
        this.key = key;
        this.value = value;
    }

    /**
     * @return The value's key
     */
    public String getKey() {
        return key;
    }

    /**
     * @return The value
     */
    public String getValue() {
        return value;
    }

    /**
     * @return The value's ModifyIndex property
     */
    public int getModifyIndex() {
        return modifyIndex;
    }

    /**
     * Decode JSON string returned by Consul Agent.
     */
    public static ConsulValue fromJson(final JSONObject json) {
        return new ConsulValue(
                json.getInt("ModifyIndex"),
                json.getString("Key"),
                Base64.decodeAsString(json.getString("Value")));
    }
}
