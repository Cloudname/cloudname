package org.cloudname.timber.client;

import org.cloudname.log.pb.Timber;
import org.cloudname.timber.server.Server;
import org.cloudname.testtools.Net;

import com.google.protobuf.ByteString;

import java.util.concurrent.CountDownLatch;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for TimberClient class.
 *
 * @author borud
 */
public class TimberClientTest {
    private Timber.LogEvent logEvent;

    @Before
    public void setUp() throws Exception {
        logEvent = Timber.LogEvent.newBuilder()
            .setTimestamp(System.currentTimeMillis())
            .setConsistencyLevel(Timber.ConsistencyLevel.BESTEFFORT)
            .setLevel(1)
            .setHost("example.com")
            .setServiceName("myservice")
            .setSource(TimberClientTest.class.getName())
            .setPid(0)
            .setTid((int) Thread.currentThread().getId())
            .setType("T")
            .setId("baluba")
            .addPayload(
                Timber.Payload.newBuilder()
                .setName("msg")
                .setPayload(ByteString.copyFromUtf8("sample logmessage")))
            .build();

    }

    @Test
    @Ignore
    public void testSimple() throws Exception {
        int port = Net.getFreePort();
        Server server = new Server(port);
        server.start();

        final CountDownLatch ackLatch = new CountDownLatch(1);

        // Set up a client with an ack listener
        TimberClient client = new TimberClient("localhost", port);
        client.addAckEventListener(new TimberClient.AckEventListener() {
                @Override
                public void ackEventReceived(Timber.AckEvent ackEvent) {
                    System.out.println(ackEvent.toString());
                    ackLatch.countDown();
                    assertEquals("baluba", ackEvent.getId(0));
                }

            });
        client.start();

        // Send a log message with an ID
        client.submitLogEvent(logEvent);

        // Wait for AckEventListener to fire.
        ackLatch.await();

        // Then shut down.
        client.shutdown();
        server.shutdown();
    }

    @Test (timeout=3000)
    public void testServerShutdown() throws Exception {
        int port = Net.getFreePort();
        Server server = new Server(port);
        server.start();

        // fire up a client
        TimberClient client = new TimberClient("localhost", port);
        client.start();

        // Send a log message with an ID
        client.submitLogEvent(logEvent);

        // Shut down the server
        server.shutdown();

        // Send the log message again, this time with the server down.
        client.submitLogEvent(logEvent);

        // Fire up log server again
        server = new Server(port);
        server.start();

        // Loop until submitting log message succeeds.
        while (! client.submitLogEvent(logEvent)) {}
    }
}
