package org.cloudname.timber.server.handler.archiver;

import org.cloudname.log.LogUtil;
import org.cloudname.log.pb.Timber;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for SimpleArchiver.
 *
 * @author borud
 */
public class SimpleArchiverTest {
    private static final long MEGABYTE = 1024 * 1024;
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    /**
     * Test initialization with existing path.
     */
    @Test public void testInitialization() throws Exception {
        final String logPath = temp.newFolder("test1").getAbsolutePath();
        final SimpleArchiver archiver = new SimpleArchiver(logPath, MEGABYTE);
        archiver.init();
    }

    /**
     * Test initialization with path that does not exist.
     */
    @Test public void testInitializationUnexist() throws Exception {
        final String logPath = temp.newFolder("test2").getAbsolutePath()
            + File.separator
            + "unexist";
        final SimpleArchiver archiver = new SimpleArchiver(logPath, MEGABYTE);
        archiver.init();
    }

    /**
     * Try to log some messages.
     */
    @Test public void testWithMessages() throws Exception {
        final String logPath = temp.newFolder("test3").getAbsolutePath();
        final SimpleArchiver archiver = new SimpleArchiver(logPath, MEGABYTE);
        archiver.init();

        final int count = 1000;

        final List<Timber.LogEvent> events = new ArrayList<Timber.LogEvent>(count);

        for (int i = 0; i < count; i++) {
            events.add(LogUtil.textEvent(10,
                                         "myservice",
                                         SimpleArchiverTest.class.getName(),
                                         "some payload " + i));
        }

        for (final Timber.LogEvent ev : events) {
            archiver.handle(ev);
        }
    }
}
