package org.cloudname.zk;

import org.cloudname.Coordinate;
import org.cloudname.Resolver;
import org.cloudname.ServiceStatus;
import org.cloudname.flags.Flag;
import org.cloudname.flags.Flags;

import java.util.ArrayList;
import java.util.List;


/**
 * Commandline tool for using the Cloudname library.
 * Flags:
 *   --operationFlag   create|delete|status|list
 *   --coordinateFlag the coordinateFlag to perform operationFlag on
 * @author dybdahl
 */
public class Main {
    @Flag(name="zooKeeperFlag", description="A list of host:port for connecting to ZooKeeper.", required=false)
    public static String zooKeeperFlag = null;

    @Flag(name="cordinateFlag", description="The coordinateFlag to work on.", required=false)
    public static String coordinateFlag = null;

    @Flag(name="operationFlag",
          description="The operationFlag to do on coordinateFlag (create, delete, status, list).", required=true)
    public static String operationFlag = null;

    /**
     *   The possible operations to do on a coordinate.
     */  
    enum Operation {
        /**
         * Invalid operation specified by the user.
         */
        NOT_VALID,
        /**
         * Create a new coordinate.
         */
        CREATE,
        /**
         * Delete a coordinate.
         */
        DELETE,
        /**
         * Print out some status about a coordinate.
         */
        STATUS,
        /**
         * Print the coordinates in zookeeper
         */
        LIST;
        public static Operation getOperation(String operationString) {
            operationString = operationString.toUpperCase();
            for (Operation operation : Operation.values()) {
                if (operation.name().equals(operationString)) {
                    return operation;
                }
            }
            return Operation.NOT_VALID;
        }
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

        Operation operation = Operation.getOperation(operationFlag);
        if (operation == Operation.NOT_VALID) {
            System.err.println("Unknown operation: " + operation);
            return;
        }
        
        ZkCloudname.Builder builder = new ZkCloudname.Builder();
        if (zooKeeperFlag == null) {
            System.out.println("Connecting to cloudname with auto connect.");
            builder.autoConnect();
        } else {
            System.out.println("Connecting to cloudname with ZooKeeper connect string " + zooKeeperFlag);
            builder.setConnectString(zooKeeperFlag);
        }
        ZkCloudname cloudname = builder.build().connect();
        System.err.println("Connected to ZooKeeper.");

        Resolver resolver = cloudname.getResolver();
        Coordinate c = Coordinate.parse(coordinateFlag);

        switch (operation) {
            case CREATE:
                cloudname.createCoordinate(c);
                System.err.println("Created coordinate.");
                break;
            case DELETE:
                cloudname.destroyCoordinate(c);
                System.err.println("Deleted coordinate.");
                break;
            case STATUS:
                ServiceStatus status = cloudname.getStatus(c);
                System.err.println("Status:\n" + status.getState().toString());
                break;
            case LIST:
                List<String> nodeList = new ArrayList<String>();
                cloudname.listRecursively(nodeList);
                for (String node : nodeList) {
                    System.err.println(node);
                }
                break;
            default:
                System.out.println("Unknown command " + operationFlag);
        }
    }
}
