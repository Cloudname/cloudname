package org.cloudname.backends.consul;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Session entity sent to/from Consul. Uses a thread to keep the session alive until it is closed
 * or the JVM terminates.
 *
 * @author stalehd@gmail.com
 */
public class ConsulSession {
    private final String endpoint;
    private final String id;
    private final String name;
    private final int ttl;
    private final int lockDelay;

    private static final Logger LOG = Logger.getLogger(ConsulSession.class.getName());
    private static final ScheduledExecutorService executor
            = Executors.newSingleThreadScheduledExecutor();
    private final Client httpClient = ClientBuilder.newClient();
    private final String behavior = "delete";
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create new session object.
     *
     * @param endpoint Consul Agent endpoint to use
     * @param id The session ID. This is not validated
     * @param name Session name. Not used for anything but human readable tag for the session.
     * @param ttl Session TTL in seconds. The session is refreshed every TTL/2 seconds.
     * @param lockDelay Internal parameter for Consul that tells how often locks can be acquired
     *                  (and reacquired). In seconds.
     */
    public ConsulSession(final String endpoint, final String id,
                         final String name, final int ttl, final int lockDelay) {
        this.endpoint = endpoint;
        this.id = id;
        this.name = name;
        this.ttl = ttl * 1000;
        this.lockDelay = lockDelay;
    }

    /**
     * The session ID.
     */
    public String getId() {
        return id;
    }

    /**
     * Session object that can be submitted to the Consul Agent.
     */
    public String toJson() {
        return new JSONObject()
                .put("Name", id)
                .put("TTL", (ttl / 1000) + "s")
                .put("LockDelay", lockDelay + "s")
                .put("Behavior", behavior)
                .toString();
    }

    /**
     * Build actual session from Consul Agent response. Only the ID field is returned, use the
     * submitted entity.
     */
    public static ConsulSession fromJsonResponse(final ConsulSession submitted, final String json) {
        try {
            final JSONObject ret = new JSONObject(json);
            // Note that the TTL returned is in milliseconds. Nice gotcha.
            return new ConsulSession(submitted.endpoint, ret.getString("ID"),
                    submitted.name, submitted.ttl / 1_000, submitted.lockDelay);
        } catch (final JSONException je) {
            LOG.log(Level.WARNING, "Couldn't grok JSON from Consul Agent. Response was " + json);
            return null;
        }
    }

    /**
     * Start the session refresh thread.
     */
    public void startKeepAlive() {
        final Entity<String> emptyEntity = Entity.entity("{}", MediaType.APPLICATION_JSON_TYPE);
        try {
            executor.scheduleAtFixedRate(() -> {
                final Response response = httpClient
                        .target(endpoint)
                        .path("/v1/session/renew")
                        .path(id).request()
                        .put(emptyEntity);
                if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                    LOG.log(Level.WARNING, "Got " + response.getStatus()
                            + " from Consul Agent when renewing sessions, exepected 200");
                }
            }, ttl / 2, ttl / 2, TimeUnit.MILLISECONDS);
        } catch (final RejectedExecutionException re) {
            // This will be thrown if the task is cancelled before it is started.
        }
    }

    /**
     * @return true if the session is closed.
     */
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Close and remove session from Consul. This will remove all KV entries tagged with this
     * session.
     */
    public void close() {
        executor.shutdownNow();
        final Entity<String> emptyEntity = Entity.entity("{}", MediaType.APPLICATION_JSON_TYPE);
        final Response response = httpClient
                .target(endpoint)
                .path("/v1/session/destroy")
                .path(id)
                .request()
                .put(emptyEntity);
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            LOG.log(Level.WARNING, "Got " + response.getStatus()
                    + " from Consult Agent when removing session, expected 200");
        }
        LOG.info("Removed session with ID " + id);
        closed.set(true);
    }
}
