package org.cloudname.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.cloudname.*;
import org.codehaus.jackson.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: dybdahl
 * Date: 12/15/11
 * Time: 9:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class ZkStatusEndpoint {
 
    private static final Logger log = Logger.getLogger(ZkStatusEndpoint.class.getName());
    private ZooKeeper zk;
    private String path;

    private int lastStatusVersion ;
    private ObjectMapper objectMapper;
    // Default service status
    private ServiceStatus status = new ServiceStatus(ServiceState.UNASSIGNED,
            "No service state has been assigned");
    private Map<String, Endpoint> endpointsByName = new HashMap<String, Endpoint>();

    public ZkStatusEndpoint(ZooKeeper zk, String path) {

        this.zk = zk;
        this.path = path;
        objectMapper = new ObjectMapper();
        lastStatusVersion = -2;  // Enforce can't write without claim.
    }
    public void load() {
        Stat stat = new Stat();
        try {
            byte[] data = zk.getData(path, false /*watcher*/, stat);

            JsonFactory jsonFactory = new JsonFactory();
            try {

                JsonParser jp = jsonFactory.createJsonParser(new String(data, Util.CHARSET_NAME));

                String statusString =objectMapper.readValue(jp, new TypeReference <String>() {});
                status = ServiceStatus.fromJson(statusString);
                endpointsByName =  objectMapper.readValue(jp,new TypeReference <Map<String, Endpoint>>() {});

            } catch (IOException e) {
                throw new CloudnameException(e);

            }
        } catch (KeeperException e) {
            throw new CloudnameException(e);

        } catch (InterruptedException e) {
            throw new CloudnameException(e);

        }
    }
    
    public void claim() {
        try {
            zk.create(
                 path, getSerializedState().getBytes(Util.CHARSET_NAME), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
            log.info("Coordinate already claimed  (" + path + ")");
            throw new CloudnameException.AlreadyClaimed(e);
        } catch (KeeperException.NoNodeException e) {
            log.info("Coordinate does not exist  (" + path + ")");
            throw new CloudnameException.CoordinateNotFound(e);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (UnsupportedEncodingException e) {
            // This is not supposed to be happening since CHARSET_NAME
            // should always be "UTF-8".
            throw new CloudnameException(e);
        }

        // Stat the status node so we have the version.  If later
        // we try to operate on the status node and we do not have
        // the correct version this can mean that someone else has
        // been meddling with the status node.  In which case we
        // must complain loudly.
        try {
            Stat stat = zk.exists(path, false);
            lastStatusVersion = stat.getVersion();
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
    }
    private String getSerializedState() {
        StringWriter stringWriter = new StringWriter();
        JsonGenerator generator;
        try {
           generator = new JsonFactory(new ObjectMapper()).createJsonGenerator(stringWriter);
        } catch (IOException e) {
            throw new CloudnameException(e);

        }
        try {
            generator.writeString(status.toJson());
            generator.writeObject(endpointsByName);
        } catch (IOException e) {
            throw new CloudnameException(e);

        }
        try {
            generator.flush();
        } catch (IOException e) {
            throw new CloudnameException(e);
        }
        return new String(stringWriter.getBuffer());
    }

    private Boolean writeStatusEndpoint() {

        try {

            Stat stat = zk.setData(path,
                        getSerializedState().getBytes(Util.CHARSET_NAME),
                        lastStatusVersion);

             lastStatusVersion = stat.getVersion();
        } catch (KeeperException.NodeExistsException e) {
            log.info("Coordinate already claimed (" + path + ")");
            throw new CloudnameException.AlreadyClaimed(e);
        } catch (KeeperException.NoNodeException e) {
            log.info("Coordinate does not exist (" + path + ")");
            throw new CloudnameException.CoordinateNotFound(e);
        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        } catch (UnsupportedEncodingException e) {
            throw new CloudnameException(e);
        }
        return true;
    }
    
    public void updateStatus(ServiceStatus status) {

        this.status = status;
        writeStatusEndpoint();
    }

    public ServiceStatus getStatus() {
        return status;
    }

    public Endpoint getEndpoint(String name) {
        return endpointsByName.get(name);
    }
    
    public void addAllEndpoints(List<Endpoint> endpoints) {
        for (Endpoint endpoint : endpointsByName.values()) {
            endpoints.add(endpoint);
        }
    }

    public void putEndpoints(List<Endpoint> newEndpoints) {
        for (Endpoint endpoint : newEndpoints) {
            if (endpointsByName.containsKey(endpoint.getName())) {
                log.info("endpoint already exists: " +  endpoint.getName());
                throw new CloudnameException.EndpointExists();
            }
            endpointsByName.put(endpoint.getName(), endpoint);
        }
        writeStatusEndpoint();
    }

    public void removeEndpoints(List<String> names) {
        for (String name : names) {
            if (! endpointsByName.containsKey(name)) {
                log.info("endpoint does not exist: " +  name);
                throw new CloudnameException.EndpointDoesNotExist();
            }
            endpointsByName.remove(name);
        }
        writeStatusEndpoint();
    }

    public void close() {
        // The nodes that are removed here are ephemeral nodes and
        // we could just let zk remove them, but on the off chance
        // that a single process would try to claim more than one
        // coordinate we provide more explicit cleanup.
        try {
            zk.delete(path, lastStatusVersion);

        } catch (KeeperException e) {
            throw new CloudnameException(e);
        } catch (InterruptedException e) {
            throw new CloudnameException(e);
        }
    }          
            
}
