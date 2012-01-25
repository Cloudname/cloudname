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
public class ZkStatusAndEndpoints implements Watcher, ZkUserInterface {
    private ZkStatusAndEndpoints.State state = ZkStatusAndEndpoints.State.EMPTY;
    private Storage storage = Storage.NO_CONNECTION;

    private int lastStatusVersion = -1000;
    private ServiceStatus serviceStatus = new ServiceStatus(ServiceState.UNASSIGNED,
            "No service state has been assigned");
    private Map<String, Endpoint> endpointsByName = new HashMap<String, Endpoint>();
    private ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger log = Logger.getLogger(ZkStatusAndEndpoints.class.getName());
    private ZooKeeper zk;
    private final String path;
    private List<CoordinateListener> coordinateListenerList =
            Collections.synchronizedList(new ArrayList<CoordinateListener>());


    
    /**
     * Constructor, the ZooKeeper instances is retrieved from implementing the ZkUserInterface so the object
     * is not ready to be used before the ZooKeeper instance is received.
     * @param path is the path of the status of the coordinate.
     */
    public ZkStatusAndEndpoints(String path) {
        this.path = path;
    }


    @Override
    public void zooKeeperDown() {
        log.info("ZkStatusAndEndpoints: Got event ZooKeeper is down.");
        synchronized (this) {
            zk = null;
            storage = Storage.NO_CONNECTION;
        }
        updateCoordinateListenersAndTakeAction(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE, "Got message from parent watcher.");
    }

    class ClaimCallback implements AsyncCallback.StringCallback {

        @Override
        public void processResult(int i, String s, Object o, String s1) {

            KeeperException.Code returnCode =  KeeperException.Code.get(i);
            ZkStatusAndEndpoints statusAndEndpoints =  (ZkStatusAndEndpoints) o;
            log.info("Claim callback " + returnCode.name() + " " + s);
           
            switch (returnCode) {

                case OK:
                    synchronized (this) {
                        storage = Storage.DIRTY;
                    }
                
                    try {
                        // todo sync this
                        if (state == State.CLAIMED) {
                        statusAndEndpoints.writeStatusEndpoint();
                        synchronized (statusAndEndpoints) {
                            statusAndEndpoints.storage = Storage.SYNCED;
                        }
                        }
                    } catch (CoordinateMissingException e) {

                        log.info("Problems writing config, coordinate missing");
                        statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                                CoordinateListener.Event.NOT_OWNER,
                                "Can not write config: " + returnCode.name() + " " + s);
                        break;
                    } catch (CloudnameException e) {
                        log.info("Problems writing config." + e.getMessage());
                            statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                                    CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE,
                                    "Can not write config after claim: " + returnCode.name() + " " + s);
                        break;
                    }
                    statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                            CoordinateListener.Event.COORDINATE_OK, "synced on recovery");
                    break;
                case NODEEXISTS:
                    synchronized (statusAndEndpoints) {
                        if (statusAndEndpoints.storage == Storage.DIRTY ||
                                statusAndEndpoints.storage == Storage.SYNCED ) {
                            break;
                        }
                    }
                  //  synchronized (this) {
                     //   storage = Storage.UNCLAIMED;
                  //  }
                    statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                            CoordinateListener.Event.NOT_OWNER,
                            "Node already exists.");
                    break;
                case NONODE:
                   // synchronized (this) {
                        //storage = Storage.UNCLAIMED;
                   // }
                    statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                            CoordinateListener.Event.NOT_OWNER,
                            "No node on claiming coordinate: " + returnCode.name() + " " + s);
                    break;
                
                default:
                    //synchronized (this) {
                     //   storage = Storage.UNCLAIMED;
                    //}
                     statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                             CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE,
                             "Could not reclaim coordinate. Return code: " + returnCode.name() + " " + s);

            }

        }
    }

    @Override
    public void newZooKeeperInstance(ZooKeeper zk) {
        log.info("ZkStatusAndEndpoints: Got new ZeeKeeper, expect session to be down so starting potential cleanup." + zk.getSessionId());
        synchronized (this) {
            this.zk = zk;
            // We always start by assuming it is unclaimed.
            storage = Storage.UNCLAIMED;
            if (state != State.CLAIMED) {
                return;
            }

            log.info("ZkStatusAndEndpoints: Reclaiming coordinate.");

            try {

                getZooKeeper().create(
                        path, serialize(serviceStatus, endpointsByName).getBytes(Util.CHARSET_NAME),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new ClaimCallback(), this);
            } catch (CloudnameException e) {
                log.info("Could not claim with the new ZooKeeper instance: " + e.getMessage());
            } catch (IOException e) {
                log.info("Got IO exception on claim with new ZooKeeper instance " + e.getMessage());
            }
        }
    }

    @Override
    public void wakeUp() {
        
        synchronized (this) {
            if (state != State.CLAIMED || storage == Storage.SYNCED || zk == null) {
                return;
            }

            try {
                zk.create(
                        path, serialize(serviceStatus, endpointsByName).getBytes(Util.CHARSET_NAME),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new ClaimCallback(), this);
            } catch (CloudnameException e) {
                log.info("Could not claim with the new ZooKeeper instance: " + e.getMessage());
            } catch (IOException e) {
                log.info("Got IO exception on claim with new ZooKeeper instance " + e.getMessage());
            }
        }
        System.out.println("alive");
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
        CLAIMED,
    }
    private enum Storage {
        NO_CONNECTION,
        UNCLAIMED,
        DIRTY,
        SYNCED,
    }


    /**
     * Updates the ServiceStatus and persists it. Only allowed if we claimed the coordinate.
     * @param status The new value for serviceStatus.
     */
    public void updateStatus(ServiceStatus status) throws CloudnameException, CoordinateMissingException {
        synchronized (this) {
            if (state != State.CLAIMED) {
                throw new IllegalStateException("This instance did not claim this coordinate.");
            }
            this.serviceStatus = status;
        }
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
    public void putEndpoints(List<Endpoint> newEndpoints)
            throws EndpointException, CloudnameException, CoordinateMissingException {
        synchronized (this) {
            if (state != State.CLAIMED) {
                throw new IllegalStateException("This instance did not claim this coordinate.");
            }
            for (Endpoint endpoint : newEndpoints) {
                if (endpointsByName.containsKey(endpoint.getName())) {
                    throw new EndpointException("endpoint already exists: " +  endpoint.getName());
                }
                endpointsByName.put(endpoint.getName(), endpoint);
            }
        }
        writeStatusEndpoint();
    }

    /**
     * Remove endpoints and persist it. Requires that this instance owns the claim to the coordinate.
     * @param names names of endpoints to be removed.
     */
    public void removeEndpoints(List<String> names)
            throws EndpointException, CloudnameException, CoordinateMissingException {
        synchronized (this) {
            if (state != State.CLAIMED) {
                throw new IllegalStateException("This instance did not claim this coordinate.");
            }
            for (String name : names) {
                if (! endpointsByName.containsKey(name)) {
                    throw new EndpointException("endpoint does not exist: " +  name);
                }
                if (null == endpointsByName.remove(name)) {
                    throw new EndpointException("End point does not exists.");
                }
            }
        }
        writeStatusEndpoint();
    }

    /**
     * Release the claim of the coordinate. It means that nobody owns the coordinate anymore.
     * Requires that that this instance owns the claim to the coordinate.
     */
    public void releaseClaim() throws CloudnameException {
        synchronized (this) {
            if (state != State.CLAIMED) {
                throw new IllegalStateException("This instance did not own the claim to this coordinate.");
            }
            try {
                getZooKeeper().delete(path, lastStatusVersion);
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
        // List is syncronized.
        coordinateListenerList.add(coordinateListener);
        fixStorageState();
    }

    /**
     * Handles even from ZooKeeper for this coordinate.
     * @param event
     */
    @Override public void process(WatchedEvent event) {
        log.info("Got an event from ZooKeeper " + event.toString());


        // If we lost connection, we don't attempt to register another watcher as this might be blocking forever.
        // Parent might try to reconnect.
        if (event.getType() == Event.EventType.None &&
                (event.getState() == Event.KeeperState.Disconnected
                        || event.getState() == Event.KeeperState.AuthFailed)) {
            updateCoordinateListenersAndTakeAction(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE, event.toString());
            return;
        }
        if (event.getType() == Event.EventType.None &&
                (event.getState() == Event.KeeperState.Expired)) {
            updateCoordinateListenersAndTakeAction(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE, event.toString());
            return;
        }

        // If node is deleted, we have no node to place a new watcher so we stop watching.
        if (event.getType() == Event.EventType.NodeDeleted) {
            updateCoordinateListenersAndTakeAction(CoordinateListener.Event.NOT_OWNER, event.toString());
            return;
        }

        if (event.getType() == Event.EventType.NodeDataChanged) {

            // todo only true for claimed coordinates

            updateCoordinateListenersAndTakeAction(CoordinateListener.Event.COORDINATE_OUT_OF_SYNC, event.toString());
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
            updateCoordinateListenersAndTakeAction(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE,
                    "Failed setting up new watcher, CloudnameException.");
            return;
        } catch (InterruptedException e) {
            updateCoordinateListenersAndTakeAction(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE,
                    "Failed setting up new watcher, InterruptedException.");
            return;
        }
        log.info("Checking state of coordinate and sending event: " + path);

    }

    /**
     * Safe way to get a ZooKeeper instance.
     * @throws CloudnameException if connection is not up.
     */
    private ZooKeeper getZooKeeper()  {
        synchronized (this) {
            return zk;
        }
    }
    /**
     * Sends an event too all coordinate listeners. Note that the event is sent from this thread so if the
     * callback code does the wrong calls, deadlocks might occur.
     * @param event
     * @param message
     */
    private void updateCoordinateListenersAndTakeAction(CoordinateListener.Event event, String message) {
        log.info("Event " + event.name() + " " + message);
        for (CoordinateListener listener : coordinateListenerList) {
            listener.onConfigEvent(event, message);
        }
        log.info("Done notifying listeners about event " + event.name() + " # listeners " + coordinateListenerList.size());
        synchronized (this) {
            // Simply ignore the action from the listener, we are ok anyway.
            if (event == CoordinateListener.Event.COORDINATE_OK) {
                storage = Storage.SYNCED;
                return;
            }

            if (event == CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE) {
                storage = Storage.NO_CONNECTION;
                return;
            }

            if (event == CoordinateListener.Event.NOT_OWNER) {
                storage = Storage.UNCLAIMED;
                try {
                    registerWatcher();
                } catch (CloudnameException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    log.info("failed setting up watcher, giving up");
                    return;
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    log.info("failed setting up watcher, giving up");
                    return;
                }
                return;
            }

            if (event == CoordinateListener.Event.COORDINATE_OUT_OF_SYNC) {
                if (state == State.CLAIMED)  {
                    try {
                        writeStatusEndpoint();
                    } catch (CoordinateMissingException e) {
                        storage = Storage.UNCLAIMED;
                        return;
                    } catch (CloudnameException e) {
                        storage = Storage.NO_CONNECTION;
                        log.info("Exceptiion cloudname" + e.getMessage());
                        return;
                    }
                }
                return;

            }
            storage = Storage.NO_CONNECTION;
        }
        System.err.println("ZkStatusAndEndpoints trying to recover.");
        //newZooKeeperInstance(getZooKeeper());
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

            byte[] data = getZooKeeper().getData(path, false /*watcher*/, stat);

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
        synchronized (this) {


        };
        if (state != State.EMPTY) {
            throw new IllegalStateException("Does not make sense to claim when something is already loaded.");
        }
        try {
            getZooKeeper().create(
                    path, serialize(serviceStatus, endpointsByName).getBytes(Util.CHARSET_NAME), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        } catch (KeeperException.NodeExistsException e) {
            log.info("Coordinate already claimed  (" + path + ")");

            throw new CoordinateAlreadyClaimedException("Coordinate already claimed.(" + path + ")");
        } catch (KeeperException.NoNodeException e) {
            log.info("Coordinate does not exist  (" + path + ")");

            throw new CoordinateMissingException("Coordinate does not exist  (" + path + ")");
        } catch (KeeperException e) {
            log.log(Level.SEVERE, "Problems with claiming" + e.getMessage());

            throw new CloudnameException(e);
        } catch (UnsupportedEncodingException e) {
            log.info("Claim: This is not supposed to be happening since CHARSET_NAME " +
               " should always be UTF-8.");

            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            log.info("claim interrupped");

            throw new CloudnameException(e);
        } catch (IOException e) {
            log.info("claim interrupped");

            throw new CloudnameException(e);
        }
        log.info("Claimed coordinate");

        // Stat the serviceStatus node so we have the version.  If later
        // we try to operate on the serviceStatus node and we do not have
        // the correct version this can mean that someone else has
        // been meddling with the serviceStatus node.  In which case we
        // must complain loudly.
        try {
            // TODO(borud, dybdahl): Consider if we need to re-read the content of the zookeeper. Can it
            // possible change between create and exists? Can we use version number to detect this or is that
            // depending on implementation details of ZooKeeper?
            Stat stat = getZooKeeper().exists(path, this);
            lastStatusVersion = stat.getVersion();
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
        state = State.CLAIMED;
        storage = Storage.DIRTY;
        return this;
    }
    class VerifyStateLoadDataCallback implements  AsyncCallback.DataCallback  {

        @Override
        public void processResult(int i, String s, Object o, byte[] loadedState, Stat stat) {
            KeeperException.Code returnCode =  KeeperException.Code.get(i);
            ZkStatusAndEndpoints statusAndEndpoints =  (ZkStatusAndEndpoints) o;
            log.info("Got callback on loading data to fix state " + returnCode.name());

            boolean diffentOwner = true;
            try {
                diffentOwner = statusAndEndpoints.getSessionId() != stat.getEphemeralOwner();
                if (diffentOwner) {
                    log.info("Different session id, will check if content is equal, if so, ignore this.");
                }
                   // statusAndEndpoints.updateCoordinateListenersAndTakeAction(CoordinateListener.Event.NOT_OWNER, "on verify");
                   // return;
                } catch (CloudnameException e1) {
                //e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
       // } catch (CloudnameException e) {
        //        statusAndEndpoints.updateCoordinateListenersAndTakeAction(CoordinateListener.Event.LOST_CONNECTION_TO_STORAGE, "on verify");
         //       return;
          //  }
            
            if  (returnCode == KeeperException.Code.OK) {
                
                // State loaded of coordinate, I am owner.
                // Try to parse the loaded data.
                Map<String, Endpoint> endpointsByNameLoaded = new HashMap<String, Endpoint>();
                try {
                    deserialize(new String(loadedState, Util.CHARSET_NAME), new ObjectMapper(), endpointsByNameLoaded);
                } catch (UnsupportedEncodingException e) {
                    statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                            CoordinateListener.Event.COORDINATE_CORRUPTED, "on verify encoding problems");
                    return;
                } catch (IOException e) {
                    statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                            CoordinateListener.Event.COORDINATE_CORRUPTED, "on verify io exception");
                    return;
                }

                // Compare the serialized representation of the in-memory model with what's in zookeeper.
                byte[] currentState;
                try {
                    currentState = serialize(serviceStatus, endpointsByName).getBytes(Util.CHARSET_NAME);
                } catch (UnsupportedEncodingException e) {
                    statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                            CoordinateListener.Event.COORDINATE_CORRUPTED, "on verify unsupported encoding");
                    return;
                } catch (CloudnameException e) {
                    statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                            CoordinateListener.Event.COORDINATE_CORRUPTED, "on verify unsupported encoding");
                    return;
                } catch (IOException e) {
                    statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                            CoordinateListener.Event.COORDINATE_CORRUPTED, "on verify unsupported encoding");
                    return;
                }
                if (Arrays.equals(loadedState, currentState)) {
                    statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                            CoordinateListener.Event.COORDINATE_OK, "on verify");
                } else {
                    if (diffentOwner) {
                        statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                                CoordinateListener.Event.NOT_OWNER, "different session id and content");
                        return;
                    }
                    statusAndEndpoints.updateCoordinateListenersAndTakeAction(
                            CoordinateListener.Event.COORDINATE_OUT_OF_SYNC, "on verify");
                }
            }
        }
    }

    public long getSessionId() throws CloudnameException {
        return getZooKeeper().getSessionId();
    }
    private void fixStorageState() {
        ZooKeeper zk =  getZooKeeper();
        if (zk == null) {
            log.info("Can not fix state, no connection to ZooKeeper.");
            return;
        }
        log.info("Loading data in attempt to fix storage state.");
        zk.getData(path, false /*watcher*/,  new VerifyStateLoadDataCallback(), this);
       
    }



    private void registerWatcher() throws CloudnameException, InterruptedException {
        try {
            getZooKeeper().exists(path, this);
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
        synchronized (this) {
            if (state != State.CLAIMED) {
                throw new IllegalStateException("This instance did not claim this coordinate.");
            }
            if (storage != Storage.DIRTY && storage != Storage.SYNCED) {
                throw new IllegalStateException("Not owner of coordinate yet: " + storage.name());
            }
            try {

                Stat stat = getZooKeeper().setData(path,
                        serialize(serviceStatus, endpointsByName).getBytes(Util.CHARSET_NAME),
                        lastStatusVersion);
                lastStatusVersion = stat.getVersion();
            } catch (KeeperException.NoNodeException e) {
                throw new CoordinateMissingException("Coordinate does not exist " + path);
            } catch (KeeperException e) {
                throw new CloudnameException("writeStatusEndpoint", e);
            } catch (UnsupportedEncodingException e) {
                throw new CloudnameException(e);
            } catch (InterruptedException e) {
                throw new CloudnameException(e);
            } catch (IOException e) {
                throw new CloudnameException(e);
            }
        }
    }
}