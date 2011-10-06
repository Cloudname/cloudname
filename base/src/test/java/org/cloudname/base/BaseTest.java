package org.cloudname.base;

import org.cloudname.log.recordstore.RecordReader;
import org.cloudname.log.pb.Timber;

import org.cloudname.testtools.TraverseFiles;

import java.util.List;
import java.util.LinkedList;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.junit.*;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;

import java.util.logging.Logger;

/**
 * Unit tests for Base.
 *
 * @author borud
 */
public class BaseTest {
    private File logDir;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Before
    public void setUp() {
        logDir = temp.newFolder("logdir-test");
    }

    @Test
    public void testSimple() throws Exception {
        Base base = new Base()
            .setLogPath(logDir.getAbsolutePath())
            .setCoordinate("0.test.testuser.dc")
            .logHandlerSetup(true)
            .setEnableHttpConsole(true);
        base.init();

        // Inject a log message to make sure we log messages.
        Logger.getLogger("dummy.logger").info("This is a test message");

        // Make sure the log path is set
        assertNotNull(base.getLogPath());
        System.out.println("The log path is: " + base.getLogPath());

        // Since we don't want to wait around for things to flush we
        // just shut down base which should cause logs to flush.
        base.shutdown();

        // There may be more than one log file due to timing so we
        // have to find the lot of them.
        List<File> logFiles = getLogFiles(logDir);

        // For each log file just read through all the records.
        for (File logFile : logFiles) {
            RecordReader rr = new RecordReader(new FileInputStream(logFile));
            Timber.LogEvent event = null;
            while ((event = rr.read()) != null) {
                System.out.println(event);
            }
        }


        // Now we make sure that we got some logging
    }


    /**
     * Find all the log files in the log directory.
     */
    private static List<File> getLogFiles(File path) throws IOException {
        final List<File> logFiles = new LinkedList<File>();

        // Traverse the log directory looking for log files
        new TraverseFiles() {
            @Override public void onFile(final File f) {
                logFiles.add(f);
            }
        }.traverse(path);

        return logFiles;
    }

}