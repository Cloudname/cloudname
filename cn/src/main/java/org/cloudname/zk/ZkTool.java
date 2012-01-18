package org.cloudname.zk;

import org.cloudname.*;
import org.cloudname.flags.Flag;
import org.cloudname.flags.Flags;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Commandline tool for using the Cloudname library.
 * Flags:
 *   --operationFlag   create|delete|status|list
 *   --coordinateFlag the coordinateFlag to perform operationFlag on
 * @author dybdahl
 */
public class ZkTool {
    @Flag(name="zooKeeper", description="A list of host:port for connecting to ZooKeeper.")
    public static String zooKeeperFlag = null;

    @Flag(name="coordinate", description="The coordinate to work on.")
    public static String coordinateFlag = null;

    @Flag(name="operation", options = Operation.class,
          description="The operationFlag to do on coordinate.", required=true)
    public static Operation operationFlag = null;

    /**
     *   The possible operations to do on a coordinate.
     */  
    public enum Operation {
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
    }

    /**
     * Matches coordinate of type: cell.user.service.instance.config.
     */
    public static final Pattern instanceConfigPattern
            = Pattern.compile("\\/cn\\/([a-z][a-z-_]*)\\/" // cell
            + "([a-z][a-z0-9-_]*)\\/" // user
            + "([a-z][a-z0-9-_]*)\\/" // service 
            + "(\\d+)\\/config\\z"); // instance
         
    
    public static void main(String[] args) throws Exception {
        // Parse the flags.
        Flags flags = new Flags()
                .loadOpts(ZkTool.class)
                .parse(args);

        // Check if we wish to print out help text
        if (flags.helpFlagged()) {
            flags.printHelp(System.out);
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

        switch (operationFlag) {
            case CREATE:
                cloudname.createCoordinate(Coordinate.parse(coordinateFlag));
                System.err.println("Created coordinate.");
                break;
            case DELETE:
                cloudname.destroyCoordinate(Coordinate.parse(coordinateFlag));
                System.err.println("Deleted coordinate.");
                break;
            case STATUS:
                Coordinate c = Coordinate.parse(coordinateFlag);
                String statusPath = ZkCoordinatePath.getStatusPath(c);
                ServiceStatus status;

                status = cloudname.getStatus(c);

                System.err.println("Status:\n" + status.getState().toString() + " " + status.getMessage());
                List<Endpoint> endpoints = resolver.resolve("all." + c.getService()
                        + "." + c.getUser() + "." + c.getCell());
                System.err.println("Endpoints:");
                for (Endpoint endpoint : endpoints) {
                    System.err.println(endpoint.getName() + "-->" + endpoint.getHost() + ":" + endpoint.getPort()
                    + " protocol:" + endpoint.getProtocol());
                }
                break;
            case LIST:
                List<String> nodeList = new ArrayList<String>();
                cloudname.listRecursively(nodeList);
                for (String node : nodeList) {
                    Matcher m = instanceConfigPattern.matcher(node);

                    // We only parse config paths, and we convert these to Cloudname coordinates to not confuse
                    // the user.
                    if (m.matches()) {
                        System.out.println(String.format("%s.%s.%s.%s", m.group(4), m.group(3),m.group(2),m.group(1)));
                    }
                }
                break;
            default:
                System.out.println("Unknown command " + operationFlag);
        }
    }
}
