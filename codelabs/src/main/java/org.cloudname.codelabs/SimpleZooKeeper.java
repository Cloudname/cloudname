package org.cloudname.codelabs;

import org.cloudname.testtools.zookeeper.EmbeddedZooKeeper;
import java.io.File;

/**
 * A simple app that runs an embedded zookeeper. This is intended for codelabs / testing.
 * @author dybdahl
 */
public class SimpleZooKeeper {
    private static EmbeddedZooKeeper ezk;
    private static int zkport = 5454;

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
        if (args.length > 1) {
            System.err.println("Max one argument, and if specified, the argument is port number.");
        } else {
            File rootDir =  createTempDir();
            if (args.length == 1) {
                zkport = Integer.parseInt(args[0]);
            }
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
}