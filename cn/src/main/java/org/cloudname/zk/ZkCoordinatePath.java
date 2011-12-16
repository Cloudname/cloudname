package org.cloudname.zk;

import org.cloudname.Coordinate;

/**
 * Created by IntelliJ IDEA.
 * User: dybdahl
 * Date: 12/13/11
 * Time: 10:44 AM
 * To change this template use File | Settings | File Templates.
 */
public class ZkCoordinatePath {

    public ZkCoordinatePath(Coordinate coordinate) {
        this.coordinate = coordinate;
    }

    private Coordinate coordinate;

    public static final String CN_STATUS_NAME = "status";

    public static final String CN_CONFIG_NAME = "config";

    // This is the path prefix used by Cloudname in ZooKeeper.
    // Anything that lives under this prefix can only be touched by
    // the Cloudname library.  If clients begin to fiddle with nodes
    // under this prefix directly, all deals are off.
    public static final String CN_PATH_PREFIX = "/cn";

    public String getRoot() {
        return coordinateAsPath(coordinate.getCell(), coordinate.getUser(), coordinate.getService(),
                coordinate.getInstance());
    }
    
    public String getStatusPath() {
        return getRoot() + "/" + CN_STATUS_NAME;
    }


    public String getConfigPath(String name) {
        if (name == null) {
            return getRoot() + "/" + CN_CONFIG_NAME;
        }
        return getRoot() + "/" +  CN_CONFIG_NAME + "/" + name;
    }

    public static String coordinateAsPath(String cell, String user, String service) {
        return CN_PATH_PREFIX + "/" + cell + "/" + user + "/" + service;
    }

    public static String coordinateAsPath(String cell, String user, String service, Integer instance) {
        return coordinateAsPath(cell, user, service) + "/" + instance.toString();
    }
}
