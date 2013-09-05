package org.cloudname.zk;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.cloudname.*;
import org.cloudname.Resolver.ResolverListener;
import org.cloudname.flags.Flag;
import org.cloudname.flags.Flags;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Command line tool for using the Cloudname library. Run with
 * <code>--help</code> option to see available flags.
 *
 * @author dybdahl
 */
public final class ZkTool {
    @Flag(name="zookeeper", description="A list of host:port for connecting to ZooKeeper.")
    private static String zooKeeperFlag = null;

    @Flag(name="coordinate", description="The coordinate to work on.")
    private static String coordinateFlag = null;

    @Flag(name="operation", options = Operation.class,
        description = "The operation to do on coordinate.")
    private static Operation operationFlag = Operation.STATUS;

    @Flag(name = "setup-file",
        description = "Path to file containing a list of coordinates to create (1 coordinate per line).")
    private static String filePath = null;

    @Flag(name = "config",
        description = "New config if setting new config.")
    private static String configFlag = "";

    @Flag(name = "resolver-expression",
        description = "The resolver expression to listen to events for.")
    private static String resolverExpression = null;

    @Flag(name = "list",
            description = "Print the coordinates in ZooKeeper.")
    private static Boolean listFlag = null;

    /**
     * List of flag names for flags that select which action the tool should
     * perform. These flags are mutually exclusive.
     */
    private static String actionSelectingFlagNames =
            "--setup-file, --resolver, --coordinate, --list";

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

    private static ZkCloudname cloudname = null;

    public static void main(final String[] args)  {

        // Disable log system, we want full control over what is sent to console.
        final ConsoleAppender consoleAppender = new ConsoleAppender();
        consoleAppender.activateOptions();
        consoleAppender.setLayout(new PatternLayout("%p %t %C:%M %m%n"));
        consoleAppender.setThreshold(Level.OFF);
        BasicConfigurator.configure(consoleAppender);

        // Parse the flags.
        Flags flags = new Flags()
                .loadOpts(ZkTool.class)
                .parse(args);

        // Check if we wish to print out help text
        if (flags.helpFlagged()) {
            flags.printHelp(System.out);
            System.out.println("Must specify one of the following options:");
            System.out.println(actionSelectingFlagNames);
            return;
        }

        checkArgumentCombinationValid(flags);

        ZkCloudname.Builder builder = new ZkCloudname.Builder();
        if (zooKeeperFlag == null) {
            builder.setDefaultConnectString();
        } else {
            builder.setConnectString(zooKeeperFlag);
        }
        try {
            cloudname = builder.build().connect();
        } catch (CloudnameException e) {
            System.err.println("Could not connect to zookeeper " + e.getMessage());
            return;
        }

        try {
            if (filePath != null) {
                handleFilepath();
            } else if (coordinateFlag != null) {
                handleCoordinateOperation();
            } else if (resolverExpression != null) {
                handleResolverExpression();
            } else if (listFlag) {
                listCoordinates();
            } else {
                System.err.println("No action specified");
            }
        } catch (Exception e) {
            System.err.println("An error occurred: " + e.getMessage());
            e.printStackTrace();
        } finally {
            cloudname.close();
        }
    }

    private static void checkArgumentCombinationValid(final Flags flags) {
        int actionSelectedCount = 0;
        final Object[] actionSelectingFlags = {
                filePath, coordinateFlag, resolverExpression, listFlag
        };
        for (Object flag: actionSelectingFlags) {
            if (flag != null) {
                actionSelectedCount++;
            }
        }
        if (actionSelectedCount != 1) {
            System.err.println("Must specify exactly one of the following options:");
            System.err.println(actionSelectingFlagNames);
            flags.printHelp(System.err);
            System.exit(1);
        }
    }

    private static void handleResolverExpression() {
        final Resolver resolver = cloudname.getResolver();
        try {
            System.out.println("Added a resolver listener for expression: " + resolverExpression + ". Will print out all events for the given expression.");
            resolver.addResolverListener(resolverExpression, new ResolverListener() {
                @Override
                public void endpointEvent(Event event, Endpoint endpoint) {
                    System.out.println("Received event: " + event + " for endpoint: " + endpoint);
                }
            });
        } catch (CloudnameException e) {
            System.err.println("Problem with cloudname: " + e.getMessage());
        }
        final BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            System.out.println("Press enter to exit");
            String s = null;
            try {
                s = br.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (s.length() == 0) {
                System.out.println("Exiting");
                System.exit(0);
            }
        }
    }

    private static void handleCoordinateOperation() {
        final Resolver resolver = cloudname.getResolver();
        final Coordinate coordinate = Coordinate.parse(coordinateFlag);
        switch (operationFlag) {
            case CREATE:
                try {
                    cloudname.createCoordinate(coordinate);
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
                    cloudname.destroyCoordinate(coordinate);
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
                ServiceStatus status;
                try {
                    status = cloudname.getStatus(coordinate);
                } catch (CloudnameException e) {
                    System.err.println("Problems loading status, is service running? Error:\n" + e.getMessage());
                    break;
                }
                System.err.println("Status:\n" + status.getState().toString() + " " + status.getMessage());
                List<Endpoint> endpoints = null;
                try {
                    endpoints = resolver.resolve("all." + coordinate.getService()
                            + "." + coordinate.getUser() + "." + coordinate.getCell());
                } catch (CloudnameException e) {
                    System.err.println("Got error: " + e.getMessage());
                    break;
                }
                System.err.println("Endpoints:");
                for (Endpoint endpoint : endpoints) {
                    if (endpoint.getCoordinate().getInstance() == coordinate.getInstance()) {
                        System.err.println(endpoint.getName() + "-->" + endpoint.getHost() + ":" + endpoint.getPort()
                                + " protocol:" + endpoint.getProtocol());
                        System.err.println("Endpoint data:\n" + endpoint.getEndpointData());
                    }
                }
                break;
                }
            case HOST: {
                List<Endpoint> endpoints = null;
                try {
                    endpoints = resolver.resolve(coordinate.asString());
                } catch (CloudnameException e) {
                    System.err.println("Could not resolve " + coordinate.asString() + " Error:\n" + e.getMessage());
                    break;
                }
                for (Endpoint endpoint : endpoints) {
                    System.out.println("Host: " + endpoint.getHost());
                }
                }
                break;
            case SET_CONFIG:
                try {
                    cloudname.setConfig(coordinate, configFlag, null);
                } catch (CloudnameException e) {
                    System.err.println("Got error: " + e.getMessage());
                    break;

                } catch (CoordinateMissingException e) {
                    System.err.println("Non-existing coordinate.");
                }
                System.err.println("Config updated.");
                break;

            case READ_CONFIG:
                try {
                    System.out.println("Config is:" + cloudname.getConfig(coordinate));
                } catch (CoordinateMissingException e) {
                    System.err.println("Non-existing coordinate.");
                } catch (CloudnameException e) {
                    System.err.println("Problem with cloudname: " + e.getMessage());
                }
                break;
            default:
                System.out.println("Unknown command " + operationFlag);
        }
    }

    private static void listCoordinates() {
        try {
            final List<String> nodeList = new ArrayList<String>();
            cloudname.listRecursively(nodeList);
            for (final String node : nodeList) {
                final Matcher m = instanceConfigPattern.matcher(node);

                /*
                 *  We only parse config paths, and we convert these to
                 *  Cloudname coordinates to not confuse the user.
                 */
                if (m.matches()) {
                    System.out.printf("%s.%s.%s.%s\n",
                            m.group(4), m.group(3), m.group(2), m.group(1));
                }
            }
        } catch (final CloudnameException e) {
            System.err.println("Got error: " + e.getMessage());
        } catch (final InterruptedException e) {
            System.err.println("Got error: " + e.getMessage());
        }
    }

    private static void handleFilepath() {
        final BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(filePath));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found: " + filePath, e);
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
            throw new RuntimeException("Failed to read coordinate from file. " + e.getMessage(), e);
        } finally {
            cloudname.close();
            try {
                br.close();
            } catch (IOException e) {
                System.err.println("Failed while trying to close file reader. " + e.getMessage());
            }
        }
    }

    // Should not be instantiated.
    private ZkTool() {}
}
