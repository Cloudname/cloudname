package org.cloudname.testtools;

import java.util.List;
import java.util.LinkedList;
import java.io.File;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for TraverseFiles.
 *
 * @author borud
 */
public class TraverseFilesTest {
    @Test
    public void simpleTraversalTest() throws Exception {
        final List<File> files = new LinkedList<File>();
        final List<File> dirs = new LinkedList<File>();

        // Traverse the source directory
        new TraverseFiles() {
            @Override
            public void onFile(final File f) {
                files.add(f);
            }

            @Override
            public void onDir(final File d) {
                dirs.add(d);
            }
        }.traverse(new File("src"));

        // Now, make sure we got something
        assertTrue(files.size() > 0);
        assertTrue(dirs.size() > 0);
    }
}