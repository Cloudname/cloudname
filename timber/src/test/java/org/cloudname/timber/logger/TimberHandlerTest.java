package org.cloudname.timber.logger;

import org.cloudname.log.pb.Timber;

import org.cloudname.timber.server.Server;
import org.cloudname.timber.server.ServerTest;
import org.cloudname.timber.server.handler.LogEventHandler;

import java.io.IOException;
import java.util.List;
import java.util.LinkedList;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for TimberHandler.
 *
 * @author borud
 */
public class TimberHandlerTest {
    private static final String logMessage = "this is the log message";

    private int serverListenPort;
    private Server server;
    private ListLogHandler listLogHandler;
    private LogRecord rec;

    /**
     * A LogEventHandler that just gathers all log messages it has
     * gotten in a list.
     */
    public static class ListLogHandler implements LogEventHandler {
        private List<Timber.LogEvent> events = new LinkedList<Timber.LogEvent>();

        public synchronized void handle(Timber.LogEvent event) {
            System.out.println("=xx================================================");
            System.out.println(event.toString());
            System.out.println("==================================================");
            events.add(event);
        }

        public synchronized List getEvents() {
            return Collections.unmodifiableList(events);
        }

        public void flush() {}
        public void close() {}
        public String getName() {
            return ListLogHandler.class.getName();
        }
    }

    /**
     * Allocate a free port and fire up an instance of the log
     * server. Also populate {@code rec} with the appropriate fields.
     */
    @Before
    public void setup() throws IOException {
        listLogHandler = new ListLogHandler();
        serverListenPort = ServerTest.getFreePort();
        server = new Server(serverListenPort);
        server.addHandler(listLogHandler);
        server.start();

        rec = new LogRecord(Level.INFO, logMessage);
    }

    /**
     * Shut down the log server.
     */
    @After
    public void teardown() {
        // server.shutdown();
    }

    @Test
    @Ignore
    public void testSimple() {
        TimberHandler handler = new TimberHandler("myservice", "localhost", serverListenPort);
        handler.publish(rec);
        assertEquals(1, listLogHandler.getEvents().size());
    }
}