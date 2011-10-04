package org.cloudname.testtools;

import java.io.File;
import java.io.IOException;

/**
 * Utility for traversing directories.  Use like this:
 *
 * <pre>
 *   new TraverseFiles() {
 *     @Override public void onFile(final File f) {
 *       // do stuff with f
 *     }
 *
 *     @Override public void onDir(final File d) {
 *       // do stuff with d
 *     }
 *
 *   }.traverse(new File("mydir"));
 * </pre>
 *
 * @author borud
 */
public class TraverseFiles {
    public final void traverse(final File f) throws IOException {
        // If we have a directory we recurse
        if (f.isDirectory()) {
            onDir(f);
            final File[] children = f.listFiles();
            for(File child : children) {
                traverse(child);
            }
            return;
        }
        onFile(f);
    }

    public void onDir(final File dir) {}
    public void onFile(final File f) {}
}