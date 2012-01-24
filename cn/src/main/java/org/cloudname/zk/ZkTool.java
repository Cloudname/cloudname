package org.cloudname.zk;

import org.cloudname.*;
import org.cloudname.flags.Flag;
import org.cloudname.flags.Flags;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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
        description = "The operationFlag to do on coordinate.")
    public static Operation operationFlag = Operation.STATUS;

    @Flag(name = "setup-file",
        description = "Path to file containing a list of coordinates to create (1 coordinate per line).")
    public static String filePath = null;

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


    public static void main(String[] args) {
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
            builder.setDefaultConnectString();
        } else {
            System.out.println("Connecting to cloudname with ZooKeeper connect string " + zooKeeperFlag);
            builder.setConnectString(zooKeeperFlag);
        }
        ZkCloudname cloudname = builder.build().connect();
        System.err.println("Connected to ZooKeeper.");

        Resolver resolver = cloudname.getResolver();

        if (filePath != null) {
            BufferedReader br;
            try {
                br = new BufferedReader(new FileReader(filePath));
            } catch (FileNotFoundException e) {
                System.err.println("File not found: " + filePath);
                return;
            }
            String line;
            try {
                while ((line = br.readLine()) != null) {
                    try {
                        cloudname.createCoordinate(Coordinate.parse(line));
                        System.out.println("Created " + line);
                    } catch (Exception e) {
                        System.err.println("Could not create: " + line);
                        e.printStackTrace();
                    }

                }
            } catch (IOException e) {
                System.err.println("Failed to read coordinate from file. " + e.getMessage());
            }
            return;
        }

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
                try {
                    status = cloudname.getStatus(c);
                } catch (CloudnameException e) {
                    System.err.println("Problems loading status node: " + statusPath);
                    System.err.println("Is the instance running?");
                    return;
                }
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
