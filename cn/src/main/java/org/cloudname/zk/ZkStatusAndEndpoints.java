package org.cloudname.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.cloudname.*;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class keeps track of serviceStatus and endpoints for a coordinate.
 * It has an inner class for building an instance (Builder).
 *
 * TODO(dybdahl): Add support for claiming an existing node. This could be used for
 * recovery after network failure etc.
 *
 * @author dybdahl
 */
public class ZkStatusAndEndpoints implements Watcher {

    /**
     * This class can build ZkStatusAndEndpoints.
     * If you want to create an instance that claims the coordinate it can be done like this:
     *   ZkStatusAndEndpoints statusAndEndpoints = new ZkStatusAndEndpoints.Builder(zk, statusPath).build().claim();
     * Instead of claim you can also load the coordinate.
     */
    public static class Builder {
        private ZkStatusAndEndpoints.State state = ZkStatusAndEndpoints.State.EMPTY;
        private ZkCloudname.ZooKeeperKeeper zk = null;
        private String path = null;
        private int lastStatusVersion = -1000;
        private ServiceStatus serviceStatus = new ServiceStatus(ServiceState.UNASSIGNED,
                "No service state has been assigned");
        private Map<String, Endpoint> endpointsByName = new HashMap<String, Endpoint>();
        private ObjectMapper objectMapper = new ObjectMapper();

        /**
         * Constructor for builder.
         * @param zk The ZooKeeper instance to use.
         * @param path The serviceStatus/endpoints path of the coordinate to claim or load.
         */
        Builder(ZkCloudname.ZooKeeperKeeper zk, String path) {
            this.zk = zk;
            this.path = path;
        }

        ZkCloudname.ZooKeeperKeeper getZooKeeperKeeper() {
            return zk;
        }

        /**
         * Returns the path.
         * @return serviceStatus/endpoints path of the coordinate.
         */
        public String getPath() {
            return path;
        }

        /**
         * Returns the serviceStatus.
         * @return serviceStatus.
         */
        public ServiceStatus getServiceStatus() {
            return serviceStatus;
        }

        /**
         * Returns the ObjectMapper instance.
         * @return ObjectMapper.
         */
        public ObjectMapper getObjectMapper() {
            return objectMapper;
        }

        /**
         * Returns the State.
         * @return state
         */
        public ZkStatusAndEndpoints.State getState() {
            return state;
        }

        /**
         * Returns the version if the coordinate was claimed.
         * @return version.
         */
        public int getLastStatusVersion() {
            return lastStatusVersion;
        }

        /**
         * Returns a list of Endpoints if loaded or an empty list of endpoints if claimed.
         * @return Map<String, Endpoint> endpoints.
         */
        public Map<String, Endpoint> getEndpointsByName() {
            return endpointsByName;
        }

        /**
         * Builds an instance of ZkStatusAndEndpoints. Requires that load() or claim() is called upfront.
         * @return this.
         */
        public ZkStatusAndEndpoints build() {
            return new ZkStatusAndEndpoints(this);
        }
    }

    /**
     * The state of the ownership of the endpoint.
     */
    private enum State {
        /**
         * We have not yet loaded or claimed the endpoint.
         */
        EMPTY,
        /**
         * We have loaded the state from ZooKeeper. It means that we are in a read-only mode.
         */
        LOADED,
        /**
         * We have claimed and own the coordinate. We are the only one that are allowed to modify.
         */
        CLAIMED
    }

    private static final Logger log = Logger.getLogger(ZkStatusAndEndpoints.class.getName());

    private State state;
    private final ZkCloudname.ZooKeeperKeeper zk;
    private final String path;
    private int lastStatusVersion;
    private final ObjectMapper objectMapper;
    private ServiceStatus serviceStatus;
    private final Map<String, Endpoint> endpointsByName;
    private List<CoordinateListener> coordinateListenerList =
                Collections.synchronizedList(new ArrayList<CoordinateListener>());

    /**
     * Updates the ServiceStatus and persists it. Only allowed if we claimed the coordinate.
     * @param status The new value for serviceStatus.
     */
    public synchronized void updateStatus(ServiceStatus status) throws CloudnameException, CoordinateMissingException {
        if (state != State.CLAIMED) {
            throw new IllegalStateException("This instance did not claim this coordinate.");
        }
        this.serviceStatus = status;
        writeStatusEndpoint();
    }

    /**
     * Returns the ServiceStatus. If we claimed the coordinate, this is the real-time value, otherwise
     * it is the last loaded value.
     * @return ServiceStatus.
     */
    public synchronized ServiceStatus getServiceStatus() {
        return serviceStatus;
    }

    /**
     * Returns the Endpoint. If we claimed the coordinate, this is the real-time value, otherwise
     * it is the last loaded value.
     * @return Endpoint.
     */
    public synchronized Endpoint getEndpoint(String name) {
        return endpointsByName.get(name);
    }

    /**
     * Set all endpoints to the endpoints argument.
     * @param endpoints The endpoints are put in this list.
     */
    public synchronized void returnAllEndpoints(List<Endpoint> endpoints) {
        for (Endpoint endpoint : endpointsByName.values()) {
            endpoints.add(endpoint);
        }
    }

    /**
     * Adds new endpoints and persist them. Requires that this instance owns the claim to the coordinate.
     * @param newEndpoints endpoints to be added.
     */
    public synchronized void putEndpoints(List<Endpoint> newEndpoints)
            throws EndpointException, CloudnameException, CoordinateMissingException {
        if (state != State.CLAIMED) {
            throw new IllegalStateException("This instance did not claim this coordinate.");
        }
        for (Endpoint endpoint : newEndpoints) {
            if (endpointsByName.containsKey(endpoint.getName())) {
                throw new EndpointException("endpoint already exists: " +  endpoint.getName());
            }
            endpointsByName.put(endpoint.getName(), endpoint);
        }
        writeStatusEndpoint();
    }

    /**
     * Remove endpoints and persist it. Requires that this instance owns the claim to the coordinate.
     * @param names names of endpoints to be removed.
     */
    public synchronized void removeEndpoints(List<String> names)
            throws EndpointException, CloudnameException, CoordinateMissingException {
        if (state != State.CLAIMED) {
            throw new IllegalStateException("This instance did not claim this coordinate.");
        }
        for (String name : names) {
            if (! endpointsByName.containsKey(name)) {
                throw new EndpointException("endpoint does not exist: " +  name);
            }
            //endpointsByName.remove(endpointsByName.get(name));
            if (null == endpointsByName.remove(name)) {
                throw new EndpointException("End point does not exists.");
            }
        }
        writeStatusEndpoint();
    }

    /**
     * Release the claim of the coordinate. It means that nobody owns the coordinate anymore.
     * Requires that that this instance owns the claim to the coordinate.
     */
    public synchronized void releaseClaim() throws CloudnameException {
        if (state != State.CLAIMED) {
            throw new IllegalStateException("This instance did not own the claim to this coordinate.");
        }
        try {

            zk.getZooKeeper().delete(path, lastStatusVersion);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        }

        serviceStatus = new ServiceStatus(ServiceState.UNASSIGNED,
                "No service state has been assigned");
        state = State.LOADED;
        endpointsByName.clear();
        lastStatusVersion = -1;
    }

    /**
     * Creates a string for debugging etc
     * @return serialized version of the instance data.
     */
    public synchronized String toString() {
        try {
            return serialize(serviceStatus, endpointsByName);
        } catch (IOException e) {
            return "Could not serialize: " + e.toString();
        } catch (CloudnameException e) {
            return "Could not serialize: " + e.toString();
        }
    }

    /**
     * Registers a coordinatelistener that will receive events when there are changes to the status node.
     * Don't do any heavy lifting in the callback and don't call cloudname from the callback as this might create
     * a deadlock.
     * @param coordinateListener
     */
    public void registerCoordinateListener(CoordinateListener coordinateListener) throws CloudnameException {
        coordinateListenerList.add(coordinateListener);
        try {
            sendCoordinateEvents(verifyState(), "Message triggered by registerCoordinateListener().");
        } catch (IOException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
    }

    /**
     * Handles even from ZooKeeper for this coordinate.
     * @param event
     */
    @Override public void process(WatchedEvent event) {
        log.info("Got an event from ZooKeeper " + event.toString());


        // If we lost connection, we don't attempt to register another watcher as this might be blocking forever.
        if (event.getType() == Event.EventType.None &&
                (event.getState() == Event.KeeperState.Disconnected
                        || event.getState() == Event.KeeperState.Expired
                        || event.getState() == Event.KeeperState.AuthFailed)) {
            sendCoordinateEvents(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE, event.toString());
            return;
        }

        // If node is deleted, we have no node to place a new watcher so we stop watching.
        if (event.getType() == Event.EventType.NodeDeleted) {
            sendCoordinateEvents(CoordinateListener.Event.COORDINATE_VANISHED, event.toString());
            return;
        }

        // Seems like we are connected, do sanity checking after we have registered a new watcher just to be safe.
        if (event.getState() == Event.KeeperState.SyncConnected) {
            log.info("Signing up for more events.");
        } else {
            log.log(Level.WARNING, "Got some events I don't know how to handle, sanity checking " +
                    "(hopefully this will not create another event):" + event.toString());
        }
        try {
            registerWatcher();
        } catch (CloudnameException e) {
            sendCoordinateEvents(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE,
                    "Failed setting up new watcher, CloudnameException.");
            return;
        } catch (InterruptedException e) {
            sendCoordinateEvents(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE,
                    "Failed setting up new watcher, InterruptedException.");
            return;
        }
        log.info("Checking state of coordinate and sending event: " + path);
        try {
            sendCoordinateEvents(verifyState(), event.toString());
        } catch (CloudnameException e) {
            sendCoordinateEvents(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE,
                    "Failed setting up new watcher.");
        } catch (InterruptedException e) {
            sendCoordinateEvents(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE,
                    "Failed setting up new watcher, InterruptedException.");
        } catch (IOException e) {
            sendCoordinateEvents(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE,
                    "Failed setting up new watcher, IOException.");
        }
    }

    /**
     * Sends an event too all coordinate listeners. Note that the event is sent from this thread so if the
     * callback code does the wrong calls, deadlocks might occur.
     * @param event
     * @param message
     */
    private void sendCoordinateEvents(CoordinateListener.Event event, String message) {
        boolean dieNow = false;
        boolean softRecover = false;
        boolean hardRecovery = false;
        for (CoordinateListener listener : coordinateListenerList) {
            switch (listener.onConfigEvent(event, message)) {
                case DO_NOTHING:
                    continue;
                case DIE_NOW:
                    dieNow = true;
                    continue;
                case DIE_IF_NORMAL_AUTO_RECOVERY_FAILS:
                    softRecover = true;
                    break;
                case DIE_IF_PANIC_AUTO_RECOVERY_FAILS:
                    hardRecovery = true;
                    break;
            }
        }
        
        // Calculate die time
        
        if (dieNow) {
            System.exit(1);
        }
        if (hardRecovery || softRecover) {
            if (!zk.reconnect()) {
                log.log(Level.SEVERE, "Did not manage to reconnect to ZooKeeper, exiting process.");
                System.exit(0);
            }
        }
        if (hardRecovery) {
            // recreate coordinate if needed
        }
        if (hardRecovery || softRecover) {
            // try write coordinate, claim coordinate
        }
    }

    /**
     * Loads the coordinate from ZooKeeper.
     * @return this.
     */
    public ZkStatusAndEndpoints load() throws CloudnameException {
        if (state != ZkStatusAndEndpoints.State.EMPTY) {
            throw new IllegalStateException("Does not make sense to load when something is already set.");
        }
        Stat stat = new Stat();
        try {

            byte[] data = zk.getZooKeeper().getData(path, false /*watcher*/, stat);

            serviceStatus = deserialize(new String(data, Util.CHARSET_NAME), objectMapper, endpointsByName);

        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (UnsupportedEncodingException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (IOException e) {
            throw new CloudnameException(e);
        }
        state = State.LOADED;

        return this;
    }

    /**
     * Claims a coordinate.
     * @return this.
     */
    public ZkStatusAndEndpoints claim() throws CloudnameException, CoordinateMissingException, CoordinateAlreadyClaimedException {
        if (state != State.EMPTY) {
            throw new IllegalStateException("Does not make sense to claim when something is already loaded.");
        }
        try {
            zk.getZooKeeper().create(
                    path, serialize(serviceStatus, endpointsByName).getBytes(Util.CHARSET_NAME), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
            log.info("Coordinate already claimed  (" + path + ")");
            throw new CoordinateAlreadyClaimedException("Coordinate already claimed.(" + path + ")");
        } catch (KeeperException.NoNodeException e) {
            throw new CoordinateMissingException("Coordinate does not exist  (" + path + ")");
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (UnsupportedEncodingException e) {
            // This is not supposed to be happening since CHARSET_NAME
            // should always be "UTF-8".
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (IOException e) {
            throw new CloudnameException(e);
        }

        // Stat the serviceStatus node so we have the version.  If later
        // we try to operate on the serviceStatus node and we do not have
        // the correct version this can mean that someone else has
        // been meddling with the serviceStatus node.  In which case we
        // must complain loudly.
        try {
            // TODO(borud, dybdahl): Consider if we need to re-read the content of the zookeeper. Can it
            // possible change between create and exists? Can we use version number to detect this or is that
            // depending on implementation details of ZooKeeper?
            Stat stat = zk.getZooKeeper().exists(path, this);
            lastStatusVersion = stat.getVersion();
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
        state = State.CLAIMED;
        return this;
    }

    /**
     * We keep this private, the correct way to build an instance is to use the Builder.
     * @param builder Contains the data for the instance.
     */
    private ZkStatusAndEndpoints(Builder builder) {
        this.zk = builder.getZooKeeperKeeper();
        this.path = builder.getPath();
        this.objectMapper = builder.getObjectMapper();
        this.lastStatusVersion = builder.getLastStatusVersion();
        this.state = builder.getState();
        this.endpointsByName = builder.getEndpointsByName();
        this.serviceStatus = builder.getServiceStatus();
    }

    /**
     * Loads data from ZooKeeper and checks that this data corresponds to the in-memory data.
     * @return the state after comparing the data.
     */
    private CoordinateListener.Event verifyState() throws CloudnameException, InterruptedException, IOException {

        // First load the data. If the data can't be loaded, return that connection is lost.
        Stat stat = new Stat();
        byte[] loadedState;
        try {
            loadedState = zk.getZooKeeper().getData(path, false /*watcher*/, stat);
        } catch (KeeperException e) {
            return CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE;
        }

        if (zk.getZooKeeper().getSessionId() != stat.getEphemeralOwner()) {
            return CoordinateListener.Event.NOT_OWNER;
        }

        // Try to parse the loaded data.
        Map<String, Endpoint> endpointsByNameLoaded = new HashMap<String, Endpoint>();
        try {
            deserialize(new String(loadedState, Util.CHARSET_NAME), new ObjectMapper(), endpointsByNameLoaded);
        } catch (UnsupportedEncodingException e) {
            return CoordinateListener.Event.COORDINATE_CORRUPTED;
        } catch (IOException e) {
            return CoordinateListener.Event.COORDINATE_CORRUPTED;
        }

        // Compare the serialized representation of the in-memory model with what's in zookeeper.
        byte[] currentState;
        try {
            currentState = serialize(serviceStatus, endpointsByName).getBytes(Util.CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            return CoordinateListener.Event.COORDINATE_CORRUPTED;
        }
        if (Arrays.equals(loadedState, currentState)) {
            return CoordinateListener.Event.COORDINATE_OK;
        } else {
            return CoordinateListener.Event.COORDINATE_OUT_OF_SYNC;
        }
    }

    private void registerWatcher() throws CloudnameException, InterruptedException {
        try {
            zk.getZooKeeper().exists(path, this);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        }
    }

    /**
     * Creates a json string based on serviceStatus and endpoints.
     * @see ZkStatusAndEndpoints#deserialize for de-serialization.
     * @param status
     * @param endpointsByName
     * @return The serialized string.
     */
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

    /**
     * Deserialize a json string into the endpointsByName and the return value.
     * @see ZkStatusAndEndpoints#serialize for de-serialization.
     * @param data The string to be deserialized.
     * @param objectMapper A helper object.
     * @param endpointsByName This map is populated with the endpoints.
     * @return the ServiceStatus in the data.                                             ´
     */

    private static ServiceStatus deserialize(
            String data, ObjectMapper objectMapper, Map<String, Endpoint> endpointsByName) throws IOException {

        JsonFactory jsonFactory = new JsonFactory();
        JsonParser jp = jsonFactory.createJsonParser(data);
        String statusString = objectMapper.readValue(jp, new TypeReference <String>() {});
        endpointsByName.clear();
        endpointsByName.putAll((Map<String, Endpoint>)objectMapper.readValue(jp,
                new TypeReference <Map<String, Endpoint>>() {}));
        return ServiceStatus.fromJson(statusString);
    }

    /**
     * Creates the serialized value of the object and stores this in ZooKeeper under the path.
     * It updates the lastStatusVersion.
     */
    private void writeStatusEndpoint() throws CoordinateMissingException, CloudnameException {
        if (state != State.CLAIMED) {
            throw new IllegalStateException("This instance did not claim this coordinate.");
        }
        try {

            Stat stat = zk.getZooKeeper().setData(path,
                    serialize(serviceStatus, endpointsByName).getBytes(Util.CHARSET_NAME),
                    lastStatusVersion);
            lastStatusVersion = stat.getVersion();
        } catch (KeeperException.NoNodeException e) {
            throw new CoordinateMissingException("Coordinate does not exist " + path);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (UnsupportedEncodingException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (IOException e) {
            throw new CloudnameException(e);
        }
    }
}