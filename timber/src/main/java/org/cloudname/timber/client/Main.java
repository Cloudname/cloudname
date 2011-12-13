package org.cloudname.timber.client;

import com.google.protobuf.ByteString;

import org.cloudname.log.pb.Timber;
import org.cloudname.timber.common.Constants;

/**
 * Client for testing (for now)
 *
 * @author borud
 */
public class Main {
    public static void main(String[] args) throws Exception {
        TimberClient client = new TimberClient("localhost",
                                               Constants.DEFAULT_TIMBER_PORT);
        client.start();

        StringBuilder buf = new StringBuilder();

        for (int k = 0; k < 100; k++) {
            buf.append("abcdefghij");
        }

        String longString = buf.toString();


        for (int i = 0; i < 10000000; i++) {
            // Create and send a message
            Timber.LogEvent event = Timber.LogEvent.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setLevel(1)
                .setHost("myhost")
                .setServiceName("someservice")
                .setSource(Main.class.getName())
                .setPid(0)
                .setTid(0)
                .setType("T")
                .addPayload(
                    Timber.Payload.newBuilder()
                    .setContentType("text/plain")
                    .setPayload(ByteString.copyFrom(longString, "UTF-8"))
                    .build()
                )
                .build();
            client.submitLogEvent(event);
        }

        System.out.println("Shutting down");
        client.shutdown();
    }
}
