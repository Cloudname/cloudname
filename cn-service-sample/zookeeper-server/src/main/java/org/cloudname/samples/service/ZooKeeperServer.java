package org.cloudname.samples.service;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;

import java.io.File;

/**
 * Embedded ZooKeeper server. For testing purposes only.
 *
 * @author stalehd@gmail.com
 */
public class ZooKeeperServer {
    private ZooKeeperServer() {
        /* utility class */
    }

    /**
     * Launch the embedded ZooKeeper server.
     */
    public static void main(final String[] args) throws Exception {
        final File dataDir = new File("./zkdata");
        if (dataDir.isDirectory()) {
            dataDir.mkdir();
        }
        final InstanceSpec instanceSpec
                = new InstanceSpec(dataDir, 2181,3888, 2888, false, 1, 500, 6000);

        final TestingServer server = new TestingServer(instanceSpec, true);
    }
}
