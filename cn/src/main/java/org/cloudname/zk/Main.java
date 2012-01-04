package org.cloudname.zk;

import org.cloudname.Coordinate;
import org.cloudname.Resolver;
import org.cloudname.flags.Flag;
import org.cloudname.flags.Flags;



/**
 * Created by IntelliJ IDEA.
 * User: dybdahl
 * Date: 04.01.12
 * Time: 10:35
 * To change this template use File | Settings | File Templates.
 */
public class Main {
    @Flag(name="zooKeeper", description="A list of host:port for connecting to ZooKeeper.", required=false)
    public static String zooKeeper = null;

    @Flag(name="cordinate", description="The coordinate to work on.", required=true)
    public static String coordinate = null;

    @Flag(name="operation", description="The operation to do on coordinate (create, delete).", required=true)
    public static String operation = null;

    /**
     *   The possible operations to do on a coordinate
     */  
    enum Operation {
        NOT_VALID,
        CREATE,
        DELETE
    }
    
    private static Operation getOperation(String operationString) {
        if (operationString.equals("delete")) {
            return Operation.DELETE;
        }
        if (operationString.equals("create")) {
            return Operation.CREATE;
        }
        return Operation.NOT_VALID;
    }
    
    public static void main(String[] args) throws Exception {
        // Parse the flags.
        Flags flags = new Flags()
                .loadOpts(Main.class)
                .parse(args);

        // Check if we wish to print out help text
        if (flags.helpFlagged()) {
            flags.printHelp(System.out);
            return;
        }

        Operation o = getOperation(operation);
        if (o == Operation.NOT_VALID) {
            System.err.println("Unknown operation: " + operation);
            return;
        }
        
        ZkCloudname.Builder builder = new ZkCloudname.Builder();
        if (zooKeeper == null) {
            System.out.println("Connecting to cloudname with auto connect.");
            builder.autoConnect();
        } else {
            System.out.println("Connecting to cloudname with ZooKeeper connect string " + zooKeeper);
            builder.setConnectString(zooKeeper);
        }
        ZkCloudname cloudname = builder.build().connect();
        System.err.println("Connected to ZooKeeper.");

        Resolver resolver = cloudname.getResolver();
        Coordinate c = Coordinate.parse(coordinate);

        if (o == Operation.CREATE) {
            cloudname.createCoordinate(c);
            System.err.println("Created coordinate.");
        }
        if (o == Operation.DELETE) {
            cloudname.close();
            System.err.println("Deleted coordinate.");
        }
    }
}
