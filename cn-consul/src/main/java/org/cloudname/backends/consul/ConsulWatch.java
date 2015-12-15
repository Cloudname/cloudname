package org.cloudname.backends.consul;

import org.json.JSONArray;
import org.json.JSONException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

/**
 * A consul watch. The watch is implemented as a HTTP poll on the KV endpoint. There might be
 * changes that slips under the radar if someone creates, then updates a KV entry since non-existing
 * KV entries just return a 404 without waiting.
 *
 * @author stalehd@gmail.com
 */
public class ConsulWatch {
    /**
     * Listener for the value notifications.
     */
    public interface ConsulWatchListener {
        /**
         * A value is added.
         *
         * @param key The name of the value
         * @param value The value
         */
        void created(final String key, final String value);

        /**
         * A vaøie is changed.
         *
         * @param key The key name
         * @param value The new value
         */
        void changed(final String key, final String value);

        /**
         * A value is removed.
         *
         * @param key The removed key
         */
        void removed(final String key);
    }

    private static final Logger LOG = Logger.getLogger(ConsulWatch.class.getName());

    private final String endpoint;
    private final String pathToWatch;
    private final Client httpClient;

    /**
     * Executor for the HTTP polling thread.
     */
    private final Executor watchExecutor = Executors.newSingleThreadExecutor();

    /**
     * This is the local copy of values. These are used to determine if values are added,
     * removed or changed.
     */
    private final Map<String, ConsulValue> currentValues = new ConcurrentHashMap<>();

    /**
     * This latch is set when the watch should terminate.
     */
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    /**
     * Create a new watch.
     * @param endpoint The Consul Agend endpoint
     * @param pathToWatch The path to watch
     */
    public ConsulWatch(final String endpoint, final String pathToWatch) {
        this.endpoint = endpoint;
        this.pathToWatch = pathToWatch;
        httpClient = ClientBuilder.newClient();
        //httpClient.register(new LoggingFilter());
    }

    /**
     * Stop the watch. This will (eventually) stop all requests.
     */
    public void stop() {
        stopLatch.countDown();
    }

    /**
     * Start watching for changes.
     */
    public void startWatching(final ConsulWatchListener listener) {
        watchExecutor.execute(() -> {
            int currentIndex = 0;
            try {
                while (!stopLatch.await(1, TimeUnit.MILLISECONDS)) {
                    final Response response = httpClient
                            .target(endpoint)
                            .path("/v1/kv")
                            .path(pathToWatch)
                            .queryParam("recurse", 1)
                            .queryParam("wait", "10s")
                            .queryParam("index", currentIndex)
                            .request().get();

                    currentIndex = Integer.parseInt(response.getHeaderString("X-Consul-Index"));

                    switch (response.getStatus()) {
                        case 200:
                            try {
                                // There's changes. Get the array of values and see if something
                                // is new, changed or removed. New ones won't be in the
                                // currentValues map, changed ones exist in the map but is
                                // different, deleted ones are removed from the map.
                                processOutput(response.readEntity(String.class), listener);
                            } catch (final JSONException je) {
                                LOG.log(Level.INFO, "Got exception parsing JSON for watch "
                                        + pathToWatch, je);
                            }
                            break;

                        case 404:
                            // Fake empty response
                            processOutput("[]", listener);
                            break;

                        default:
                            // Something went wrong. Stop the watch
                            LOG.log(Level.WARNING, "Got response " + response.getStatus()
                                    + ":" + response.readEntity(String.class)
                                    + " from Consul Agent when watching " + pathToWatch
                                    + ". Stopping watch");
                            return;
                    }
                    response.close();
                }
            } catch (final InterruptedException ie) {
                LOG.log(Level.WARNING, "Got InterruptedException. Stopping watch", ie);
            }
        });
    }

    /**
     * Process the returned list from Consul.
     */
    private void processOutput(final String output, final ConsulWatchListener listener) {
        // Keep track of the values returned by the set.
        final Set<String> existingValues = new HashSet<>();
        existingValues.addAll(currentValues.keySet());
        final JSONArray array = new JSONArray(output);
        for (int i = 0; i < array.length(); i++) {
            final ConsulValue value = ConsulValue.fromJson(array.getJSONObject(i));
            final ConsulValue oldValue = currentValues.get(value.getKey());

            if (oldValue == null) {
                invokeListener(() -> listener.created(value.getKey(), value.getValue()));
                currentValues.put(value.getKey(), value);
            } else if (oldValue.getModifyIndex() != value.getModifyIndex()) {
                invokeListener(() -> listener.changed(value.getKey(), value.getValue()));
                currentValues.put(value.getKey(), value);
            }

            existingValues.remove(value.getKey());
        }

        // remove all other values
        for (final String key : existingValues) {
            currentValues.remove(key);
            invokeListener(() -> listener.removed(key));
        }
    }

    /**
     * Invoke the listener, catching any surprise exceptions.
     */
    private void invokeListener(final Runnable call) {
        try {
            call.run();
        } catch (final RuntimeException ex) {
            LOG.log(Level.WARNING, "Got RuntimeException when invoking listener for path "
                    + pathToWatch, ex);
        }

    }

}
