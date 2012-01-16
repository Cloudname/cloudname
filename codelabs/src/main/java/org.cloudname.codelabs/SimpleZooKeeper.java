package org.cloudname.codelabs;

import org.cloudname.flags.Flag;
import org.cloudname.flags.Flags;
import org.cloudname.testtools.zookeeper.EmbeddedZooKeeper;
import org.cloudname.zk.ZkTool;

import java.io.File;

/**
 * A simple app that runs an embedded zookeeper. This is intended for codelabs / testing.
 * @author dybdahl
 */
public class SimpleZooKeeper {
    private static EmbeddedZooKeeper ezk;
    @Flag(name="zkport", description="The port to use for serving zookeeper..")
    public static int zkport = 5454;

    /**
     * Delete directory with content.
     * @param path to be deleted.
     */
    static public void deleteDirectory(File path) {
        for(File f : path.listFiles())
        {
            if(f.isDirectory()) {
                deleteDirectory(f);
                f.delete();
            } else {
                f.delete();
            }
        }
        path.delete();
    }

    /**
     * Deletes and recreates a temp dir. Sets deleteOnExit().
     * @return
     */
    public static File createTempDir() {
        File baseDir = new File(System.getProperty("java.io.tmpdir"));
        File tempDir = new File(baseDir, "SimpleZooKeeper");
        if (tempDir.exists()) {
            System.err.println("Deleting old instance on startup.");
            deleteDirectory(tempDir);
        }
        tempDir.mkdir();
        tempDir.deleteOnExit();
        return tempDir;
    }

    public static void main(String[] args) throws Exception {
        // Parse the flags.
        Flags flags = new Flags()
                .loadOpts(SimpleZooKeeper.class)
                .parse(args);

        // Check if we wish to print out help text
        if (flags.helpFlagged()) {
            flags.printHelp(System.out);
            return;
        }
        File rootDir =  createTempDir();
        ezk = new EmbeddedZooKeeper(rootDir, zkport);
        try {
            ezk.init();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        System.out.println("SimpleZooKeeper running on port " + zkport);
        Thread.sleep(Long.MAX_VALUE);
    }
}