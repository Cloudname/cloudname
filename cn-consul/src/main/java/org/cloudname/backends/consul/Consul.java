package org.cloudname.backends.consul;

import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.internal.util.Base64;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Consul interface. We have to roll our own since the existing libraries doesn't support watches
 * in any way but that's OK. We only use parts of Consul anyways. This implementation doesn't use
 * Consul's service endpoints since they can't store any data besides a single host:port entry.
 *
 * <p>Temporary leases (aka ephemeral values) are created using sessions; a session is created with
 * a keep-alive thread that pokes Consul every N seconds. The session flags are set to "delete"
 * which will remove the entries tagged with that session in the KV store when the session expires.
 * In Consul lingo this is a lock on a particular value. Pay close attention to the LockDelay
 * parameter when using this class since it tells us how often the ephemeral values can be updated
 * by the clients.
 *
 * <p>Permanent leases are just plain entries into the KV store.
 *
 * <p>TODO: Use single session when creating leases.
 *
 * @author stalehd@gmail.com
 */
public class Consul {
    private static final Logger LOG = Logger.getLogger(Consul.class.getName());

    public static final int DEFAULT_LOCK_DELAY = 15;

    private final String endpoint;
    private final Client httpClient;

    /**
     * Create new backend with the specified endpoint address.
     */
    public Consul(final String endpoint) {
        this.endpoint = endpoint;
        httpClient = ClientBuilder.newClient();
        httpClient.property(ClientProperties.CONNECT_TIMEOUT, 1000);
        httpClient.property(ClientProperties.READ_TIMEOUT, 180000);
        // Uncomment this for detailed logging. You are probably desperate by now, like I've been.
        //httpClient.register(new LoggingFilter());
    }

    /**
     * Check if it is a valid endpoint. This will do a request at the KV stores root entry and
     * if it doesn't return 400 Bad Request the agent is probably not running at the specified
     * endpoint.
     */
    public boolean isValid() {
        try {
            final Response response = httpClient
                    .target(endpoint)
                    .path("/v1/kv/")
                    .request()
                    .get();
            return response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode();
        } catch (final Exception ce) {
            return false;
        }
    }

    /**
     * Create a session, PUT it to /v1/sessions/create, then build a complete session object
     * with the returned ID.
     *
     * @param name Name of session. The name does not carry any particular semantics but makes
     *             it easier to correlate the sessions with the values in the KV store for
     *             outsiders rummaging around in Consul.
     *
     * @param ttlMs Session TTL. The frequency of keep-alive calls to Consul made by the session
     *              object. In milliseconds.
     *
     * @param lockDelay Delay between allowing locks to be set. This affects the speed at which
     *                  you can set the ephemeral values. Normally this is 15 seconds but for
     *                  tests you might want to set it lower. Very scarce documentation at
     *                  https://www.consul.io/docs/agent/http/session.html, more detailed at
     *                  https://www.consul.io/docs/internals/sessions.html (but nothing explaining
     *                  the purpose of this parameter. The explanation can most likely be found in
     *                  http://research.google.com/archive/chubby.html
     */
    public ConsulSession createSession(final String name, final int ttlMs, final int lockDelay) {
        // TODO: move http stuff into the session class.
        final ConsulSession newSession
                = new ConsulSession(this.endpoint, "id", name, ttlMs, lockDelay);
        final String sessionString = newSession.toJson();
        final Entity<String> entity = Entity.entity(sessionString, MediaType.APPLICATION_JSON);
        final Response response = httpClient
                .target(endpoint)
                .path("/v1/session/create")
                .request()
                .accept(MediaType.APPLICATION_JSON)
                .put(entity);
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            LOG.log(Level.WARNING, "Expected 200 when creating session "
                    + sessionString + " but got " + response.getStatus() + ". Consul Agent"
                    + " responded with " + response.readEntity(String.class));
            return null;
        }

        final ConsulSession session
                = ConsulSession.fromJsonResponse(newSession, response.readEntity(String.class));
        if (session != null) {
            session.startKeepAlive();
        }
        return session;

    }

    /**
     * Write ephemeral data to the KV store, linked to the session. This will also work if the
     * value doesn't exist up front.
     */
    public boolean writeSessionData(final String name, final String data, final String sessionId) {
        final Response response = httpClient
                .target(endpoint)
                .path("/v1/kv/").path(name)
                .queryParam("acquire", sessionId)
                .request()
                .put(Entity.text(data));
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            LOG.log(Level.WARNING, "Could not write value " + name + "=" + data
                    + " for session " + sessionId + " got response " + response.getStatus()
                    + " but expected 200. Consul Agent says " + response.readEntity(String.class));
            return false;
        }
        if (response.readEntity(String.class).equals("true")) {
            return true;
        }
        return false;
    }

    /**
     * Create a new (permanent) entry in the KV store. Fails if the entry already exists.
     */
    public boolean createPermanentData(final String name, final String data) {
        final Response response = httpClient
                .target(endpoint)
                .path("/v1/kv/").path(name)
                .queryParam("cas", "0")
                .request()
                .put(Entity.text(data));
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            LOG.log(Level.WARNING, "Could not create permanent value " + name + "/" + data
                    + " got response " + response.getStatus() + " but expected 200");
            return false;
        }
        // Well. THIS is ugly. Never mind the return code (409 anyone) but use a f--ing string
        // to return the status. To top it off Consul says it is json.
        // (*bangs head agains wall*)
        final String result = response.readEntity(String.class);
        if (result.equals("true")) {
            return true;
        }
        return false;
    }

    /**
     * Write to KV store in Consul.
     */
    public boolean writePermanentData(
            final String name, final String data) {
        final Response response = httpClient
                .target(endpoint)
                .path("/v1/kv/").path(name)
                .request()
                .put(Entity.text(data));
        LOG.info("Wrote " + data + " to " + name);
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            LOG.log(Level.WARNING, "Could not write permanent value " + name + "/" + data
                    + " got response " + response.getStatus() + " but expected 200");
            return false;
        }
        return true;
    }

    /**
     * Remove permanent value.
     */
    public boolean removePermanentData(final String name) {
        final Response response = httpClient
                .target(endpoint)
                .path("/v1/kv/").path(name)
                .request()
                .delete();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            LOG.log(Level.WARNING, "Could not remove permanent value " + name
                    + ". Got response " + response.getStatus() + " but expected 200");
            return false;
        }
        return true;
    }

    /**
     * Read value from KV store. Value must exist.
     *
     * @return null if not found
     */
    public String readData(final String name) {
        final Response response = httpClient
                .target(endpoint)
                .path("/v1/kv/").path(name)
                .request(MediaType.APPLICATION_JSON).get();
        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            LOG.log(Level.WARNING, "Got " + response.getStatus()
                    + " from Consul Agent when querying for key named " + name);
            return null;
        }
        final String dataJson = response.readEntity(String.class);
        try {
            final JSONArray array = new JSONArray(dataJson);
            final JSONObject json = array.getJSONObject(0);
            return Base64.decodeAsString(json.getString("Value"));
        } catch (final JSONException je) {
            LOG.log(Level.WARNING, "Couldn't grok JSON from Consul Agent for value "
                    + name + ": " + dataJson);
            return null;
        }
    }

    /**
     * Create a new watch object on the specified path. Note that the watch isn't started
     * automatically. Start it manually to ensure you receive all callbacks.
     */
    public ConsulWatch createWatch(final String pathToWatch) {
        return new ConsulWatch(endpoint, pathToWatch);
    }
}
