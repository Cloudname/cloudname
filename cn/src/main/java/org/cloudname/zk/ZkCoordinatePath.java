package org.cloudname.zk;

import org.cloudname.Coordinate;


/**
 * A class for creating paths for ZooKeeper.
 * The semantic of a path is string of the form /cn/%cell%/%user%/%service%/%instance%/[status]|[config/%name%]

 * The prefix /cn indicates that the content is owned by the CloudName library.
 * Anything that lives under this prefix can only be touched by the Cloudname library.
 * If clients begin to fiddle with nodes under this prefix directly, all deals are off.
 * @author: dybdahl
 */
public class ZkCoordinatePath {
    private static final String CN_PATH_PREFIX = "/cn";
    private static final String CN_STATUS_NAME = "status";
    private static final String CN_CONFIG_NAME = "config";

    public static String getCloudnameRoot() {
        return CN_PATH_PREFIX;
    }
    /**
     * Builds the root path of a coordinate.
     * @param coordinate
     * @return the path of the coordinate in ZooKeeper (/cn/%cell%/%user%/%service%/%instance%).
     */
    public static String getCoordinateRoot(Coordinate coordinate) {
        return coordinateAsPath(coordinate.getCell(), coordinate.getUser(), coordinate.getService(),
                coordinate.getInstance());
    }

    /**
     * Builds the status path of a coordinate.
     * @param coordinate
     * @return full status path (/cn/%cell%/%user%/%service%/%instance%/status)
     */
    public static String getStatusPath(Coordinate coordinate) {
        return getCoordinateRoot(coordinate) + "/" + CN_STATUS_NAME;
    }

    /**
     * Builds the config path of a coordinate.
     * @param coordinate
     * @param name if null, the last path of the path (/%name%) is not included.
     * @return config path /cn/%cell%/%user%/%service%/%instance%/config or
     * /cn/%cell%/%user%/%service%/%instance%/config/%name%
     */
    public static String getConfigPath(Coordinate coordinate, String name) {
        if (name == null) {
            return getCoordinateRoot(coordinate) + "/" + CN_CONFIG_NAME;
        }
        return getCoordinateRoot(coordinate) + "/" +  CN_CONFIG_NAME + "/" + name;
    }

    /**
     * Builds first part of a ZooKeeper path.
     * @param cell
     * @param user
     * @param service
     * @return path (/cn/%cell%/%user%/%service%)
     */
    public static String coordinateWithoutInstanceAsPath(String cell, String user, String service) {
        return CN_PATH_PREFIX + "/" + cell + "/" + user + "/" + service;
    }

    public static String getStatusPath(String cell, String user, String service, Integer instance) {
        return coordinateAsPath(cell, user, service, instance) + "/" + CN_STATUS_NAME;
    }
    
    /**
     * Builds first part of a ZooKeeper path.
     * @param cell
     * @param user
     * @param service
     * @param instance
     * @return path (/cn/%cell%/%user%/%service%/%instance%)
     */
    private static String coordinateAsPath(String cell, String user, String service, Integer instance) {
        return coordinateWithoutInstanceAsPath(cell, user, service) + "/" + instance.toString();
    }
}
