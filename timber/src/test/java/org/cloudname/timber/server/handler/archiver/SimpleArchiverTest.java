package org.cloudname.timber.server.handler.archiver;

import org.cloudname.log.pb.Timber;

import org.cloudname.timber.server.handler.LogEventHandler;
import org.cloudname.log.LogUtil;

import com.google.protobuf.ByteString;

import java.io.File;

import java.util.List;
import java.util.ArrayList;

import org.junit.*;
import static org.junit.Assert.*;
import org.junit.rules.TemporaryFolder;

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
        String logPath = temp.newFolder("test1").getAbsolutePath();
        SimpleArchiver archiver = new SimpleArchiver(logPath, MEGABYTE);
        archiver.init();
    }

    /**
     * Test initialization with path that does not exist.
     */
    @Test public void testInitializationUnexist() throws Exception {
        String logPath = temp.newFolder("test2").getAbsolutePath()
            + File.separator
            + "unexist";
        SimpleArchiver archiver = new SimpleArchiver(logPath, MEGABYTE);
        archiver.init();
    }

    /**
     * Try to log some messages.
     */
    @Test public void testWithMessages() throws Exception {
        String logPath = temp.newFolder("test3").getAbsolutePath();
        SimpleArchiver archiver = new SimpleArchiver(logPath, MEGABYTE);
        archiver.init();

        final int count = 1000;

        List<Timber.LogEvent> events = new ArrayList<Timber.LogEvent>(count);

        for (int i = 0; i < count; i++) {
            events.add(LogUtil.textEvent(10,
                                         "myservice",
                                         SimpleArchiverTest.class.getName(),
                                         "some payload " + i));
        }

        for (Timber.LogEvent ev : events) {
            archiver.handle(ev);
        }
    }
}
