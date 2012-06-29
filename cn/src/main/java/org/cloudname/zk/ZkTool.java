package org.cloudname.zk;


import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.cloudname.*;
import org.cloudname.flags.Flag;
import org.cloudname.flags.Flags;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
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
public final class ZkTool {
    @Flag(name="zookeeper", description="A list of host:port for connecting to ZooKeeper.")
    private static String zooKeeperFlag = null;

    @Flag(name="coordinate", description="The coordinate to work on.")
    private static String coordinateFlag = null;

    @Flag(name="operation", options = Operation.class,
        description = "The operationFlag to do on coordinate.")
    private static Operation operationFlag = Operation.STATUS;

    @Flag(name = "setup-file",
        description = "Path to file containing a list of coordinates to create (1 coordinate per line).")
    private static String filePath = null;

    @Flag(name = "config",
        description = "New config if setting new config.")
    private static String configFlag = "";
    
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
         * Print the host of a coordinate.
         */
        HOST,
        /**
         * Print the coordinates in zookeeper
         */
        LIST,
        /**
         * Set config
         */
        SET_CONFIG,
        /**
         * Read config
         */
        READ_CONFIG;
    }

    /**
     * Matches coordinate of type: cell.user.service.instance.config.
     */
    public static final Pattern instanceConfigPattern
            = Pattern.compile("\\/cn\\/([a-z][a-z-_]*)\\/" // cell
            + "([a-z][a-z0-9-_]*)\\/" // user
            + "([a-z][a-z0-9-_]*)\\/" // service
            + "(\\d+)\\/config\\z"); // instance


    public static void main(final String[] args)  {

        // Disable log system, we want full control over what is sent to console.
        final ConsoleAppender consoleAppender = new ConsoleAppender();
        consoleAppender.activateOptions();
        consoleAppender.setLayout(new PatternLayout("%p %t %C:%M %m%n"));
        consoleAppender.setWriter(new Writer() {
            @Override
            public void write(char[] chars, int i, int i1) throws IOException { }

            @Override
            public void flush() throws IOException { }

            @Override
            public void close() throws IOException { }
        });
        BasicConfigurator.configure(consoleAppender);
        
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
            builder.setDefaultConnectString();
        } else {
            builder.setConnectString(zooKeeperFlag);
        }
        ZkCloudname cloudname = null;
        try {
            cloudname = builder.build().connect();
        } catch (CloudnameException e) {
            System.err.println("Could not connect to zookeeper " + e.getMessage());
            return;
        }

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
                        System.err.println("Could not create: " + line + "Got error: " + e.getMessage());
                    }
                }
            } catch (IOException e) {
                System.err.println("Failed to read coordinate from file. " + e.getMessage());
                try {
                    cloudname.close();
                } catch (InterruptedException e1) {
                    return;
                }
                return;
            } finally {
                try {
                    cloudname.close();
                } catch (InterruptedException e) {
                    System.err.println("Failed while trying to close cloudname connection. " + e.getMessage());
                }
                try {
                    br.close();
                } catch (IOException e) {
                    System.err.println("Failed while trying to close file reader. " + e.getMessage());
                }
            }
            return;
        }

        switch (operationFlag) {
            case CREATE:
                try {
                    cloudname.createCoordinate(Coordinate.parse(coordinateFlag));
                } catch (CloudnameException e) {
                    System.err.println("Got error: " + e.getMessage());
                    break;
                } catch (CoordinateExistsException e) {
                    e.printStackTrace();
                    break;
                }
                System.err.println("Created coordinate.");
                break;
            case DELETE:
                try {
                    cloudname.destroyCoordinate(Coordinate.parse(coordinateFlag));
                } catch (CoordinateDeletionException e) {
                    System.err.println("Got error: " + e.getMessage());
                    return;
                } catch (CoordinateMissingException e) {
                    System.err.println("Got error: " + e.getMessage());
                    break;
                } catch (CloudnameException e) {
                    System.err.println("Got error: " + e.getMessage());
                    break;
                }
                System.err.println("Deleted coordinate.");
                break;
            case STATUS: {
                Coordinate c = Coordinate.parse(coordinateFlag);
                ServiceStatus status;

                try {
                    status = cloudname.getStatus(c);
                } catch (CloudnameException e) {
                    System.err.println("Problems loading status, is service running? Error:\n" + e.getMessage());
                    break;
                }

                System.err.println("Status:\n" + status.getState().toString() + " " + status.getMessage());
                List<Endpoint> endpoints = null;
                try {
                    endpoints = resolver.resolve("all." + c.getService()
                            + "." + c.getUser() + "." + c.getCell());
                } catch (CloudnameException e) {
                    System.err.println("Got error: " + e.getMessage());
                    break;
                }
                System.err.println("Endpoints:");
                for (Endpoint endpoint : endpoints) {
                    if (endpoint.getCoordinate().getInstance() == c.getInstance()) {
                        System.err.println(endpoint.getName() + "-->" + endpoint.getHost() + ":" + endpoint.getPort()
                                + " protocol:" + endpoint.getProtocol());
                    }
                }
            }
            break;
            case HOST: {
                Coordinate c = Coordinate.parse(coordinateFlag);
                List<Endpoint> endpoints = null;
                try {
                    endpoints = resolver.resolve(c.asString());
                } catch (CloudnameException e) {
                    System.err.println("Could not resolve " + c.asString() + " Error:\n" + e.getMessage());
                    break;
                }
                for (Endpoint endpoint : endpoints) {
                    System.out.println("Host: " + endpoint.getHost());
                }

            }
            break;
            case LIST:
                List<String> nodeList = new ArrayList<String>();
                try {
                    cloudname.listRecursively(nodeList);
                } catch (CloudnameException e) {
                    System.err.println("Got error: " + e.getMessage());
                    break;
                } catch (InterruptedException e) {
                    System.err.println("Got error: " + e.getMessage());
                    break;
                }
                for (String node : nodeList) {
                    Matcher m = instanceConfigPattern.matcher(node);

                    // We only parse config paths, and we convert these to Cloudname coordinates to not confuse
                    // the user.
                    if (m.matches()) {
                        System.out.println(String.format("%s.%s.%s.%s", m.group(4), m.group(3), m.group(2), m.group(1)));
                    }
                }
                break;
            default:
                System.out.println("Unknown command " + operationFlag);
            case SET_CONFIG:
                Coordinate c = Coordinate.parse(coordinateFlag);
                try {
                    cloudname.setConfig(c, configFlag, null);
                } catch (CloudnameException e) {
                    System.err.println("Got error: " + e.getMessage());
                    break;

                } catch (CoordinateMissingException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                System.err.println("Created coordinate.");
                break;

            case READ_CONFIG:
                break;
        }
        try {
            cloudname.close();
        } catch (InterruptedException e) {
            return;
        }
    }

    // Should not be instantiated.
    private ZkTool() {}
}
