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
    private Server server;
    private int port;
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

        port = Net.getFreePort();
        server = new Server(port);
        server.start();
    }

    @After
    public void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    public void testSimple() throws Exception {
        final CountDownLatch ackLatch = new CountDownLatch(1);

        TimberClient client = new TimberClient("localhost", port);
        client.start();

        // Add listener.
        client.addAckEventListener(new TimberClient.AckEventListener() {
                @Override
                public void ackEventReceived(Timber.AckEvent ackEvent) {
                    System.out.println(ackEvent.toString());
                    ackLatch.countDown();
                    assertEquals("baluba", ackEvent.getId(0));
                }

            });
        client.submitLogEvent(logEvent);

        // Wait for AckEventListener to fire.
        ackLatch.await();

        // Then shut down.
        client.shutdown();
    }
}