package org.cloudname.core;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Manager class to aid in creating backends. The backend implementations should make the
 * constructor private and register through the @link{register()} method.
 */
public class BackendManager {

    /**
     * Small helper class that splits URLs into names and connection strings.
     */
    private static class BackendUrl {
        private final Pattern PATTERN = Pattern.compile("([a-z]*)://(.*)");
        final String name;
        final String connectionString;

        public BackendUrl(final String url) {
            final Matcher matcher = PATTERN.matcher(url);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Not a proper backend URL: " + url);
            }
            name = matcher.group(1);
            connectionString = matcher.group(2);
        }
    }

    private static final Map<String, Function<String, CloudnameBackend>> drivers
            = new ConcurrentHashMap<>();

    static {
        final ServiceLoader<BackendMetadata> serviceLoader
                = ServiceLoader.load(BackendMetadata.class);
        serviceLoader.forEach(
                (metadata) -> register(metadata.getName(), metadata.getFactoryMethod()));
    }

    private BackendManager() {
        /* utility class */
    }

    /**
     * Create a new driver based on the connection URL. The connection URL is on the format
     * <pre>
     *    [backend]://[backend-dependent connection string]
     * </pre>
     */
    public static CloudnameBackend getBackend(final String url) {
        if (url == null) {
            return null;
        }

        final BackendUrl backendUrl = new BackendUrl(url);
        if (!drivers.containsKey(backendUrl.name)) {
            return null;
        }
        return drivers.get(backendUrl.name).apply(backendUrl.connectionString);
    }

    /**
     * Register a new (or existing) driver
     * @param backendIdentifier Name of backend. A string identifier used in the driver URL
     * @param createMethod Method to create backend based on the URL.
     */
    public static void register(
            final String backendIdentifier, final Function<String, CloudnameBackend> createMethod) {
        if (backendIdentifier == null || createMethod == null) {
            return;
        }
        drivers.put(backendIdentifier, createMethod);
    }

    /**
     * Deregister backend
     */
    public static void deregister(final String backendIdentifer) {
        drivers.remove(backendIdentifer);
    }
}
