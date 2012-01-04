package org.cloudname.zk;

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

    @Flag(name="cordinate", description="The coordinate to work on.", required=false)
    public static String coordinate = null;

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
        
        ZkCloudnameBuilder builder = new ZkCloudnameBuilder();
        if (zooKeeper == null) {
            System.out.println("Connecting to cloudname with auto connect.");
            builder.autoConnect();
        } else {
            System.out.println("Connecting to cloudname with ZooKeeper connect string " + zooKeeper);
            builder.setConnectString(zooKeeper);
        }
        ZkCloudname cloudname = builder.
        System.err.println("Connected");

    }
}
