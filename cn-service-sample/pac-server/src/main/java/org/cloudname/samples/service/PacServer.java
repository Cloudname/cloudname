package org.cloudname.samples.service;

import static spark.Spark.init;
import static spark.Spark.port;
import static spark.Spark.staticFileLocation;
import static spark.Spark.webSocket;

import org.cloudname.core.BackendManager;
import org.cloudname.flags.Flag;
import org.cloudname.flags.Flags;

import org.cloudname.service.CloudnameService;
import org.cloudname.service.Endpoint;
import org.cloudname.service.InstanceCoordinate;
import org.cloudname.service.ServiceCoordinate;
import org.cloudname.service.ServiceData;
import org.cloudname.service.ServiceHandle;
import org.cloudname.service.ServiceListener;
import org.json.JSONObject;

import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * The server hosting the web page. Static pages (on the root) are served out of the resources,
 * web socket is served through /messages.
 */
public class PacServer {
    private static final Logger LOG = Logger.getLogger(PacServer.class.getName());
    private final int httpPort = 4567;

    @Flag (name = "cloudname-url", description = "Cloudname URL", required = true)
    private static String cloudnameUrl = null;

    @Flag (name = "coordinate", description = "Service coordinate", required = false)
    private static String myCoordinate = "pacman.test.local";

    private final CloudnameService service;

    public static final NotificationPublisher publisher = new NotificationPublisher();

    private PacServer() {
        service = new CloudnameService(BackendManager.getBackend(cloudnameUrl));
    }

    private String getCreateNotification(
            final InstanceCoordinate coordinate, final ServiceData serviceData) {
        final Endpoint ep = serviceData.getEndpoint("http");
        return new JSONObject()
                .put("coordinate", coordinate.toCanonicalString())
                .put("action", "created")
                .put("host", ep != null ? ep.getHost() : null)
                .put("port", ep != null ? ep.getPort() : null)
                .toString();
    }

    private String getRemoveNotification(final InstanceCoordinate coordinate) {
        return new JSONObject()
                .put("coordinate", coordinate.toCanonicalString())
                .put("action", "removed")
                .toString();
    }

    private void connectToCloudname() {
        final ServiceData myServiceData = new ServiceData();
        myServiceData.addEndpoint(new Endpoint("http", "0.0.0.0", httpPort));

        try (final ServiceHandle handle = service.registerService(
                ServiceCoordinate.parse(myCoordinate), myServiceData)) {

            final String[] ghostNames = new String[]{"pinky", "blinky", "inky", "clyde"};
            for (final String name : ghostNames) {
                final ServiceCoordinate ghostCoordinate = new ServiceCoordinate.Builder()
                        .fromCoordinate(ServiceCoordinate.parse(myCoordinate))
                        .setService(name)
                        .build();
                LOG.info("Listening for " + ghostCoordinate);
                service.addServiceListener(ghostCoordinate, new ServiceListener() {
                    @Override
                    public void onServiceCreated(
                            final InstanceCoordinate coordinate, final ServiceData serviceData) {
                        LOG.info("Service " + coordinate.toCanonicalString()
                                + " with serviceData " + serviceData + " is created");
                        publisher.publish(getCreateNotification(coordinate, serviceData));
                    }

                    @Override
                    public void onServiceDataChanged(
                            final InstanceCoordinate coordinate, final ServiceData data) {
                        LOG.info("Service data changed for: " + coordinate.toCanonicalString()
                                + " to: " + data.toString());
                    }

                    @Override
                    public void onServiceRemoved(final InstanceCoordinate coordinate) {
                        LOG.info("Service " + coordinate.toCanonicalString() + " was removed");
                        publisher.publish(getRemoveNotification(coordinate));
                    }
                });
            }

            LOG.info("Connected, using coordinate " + handle.getCoordinate().toCanonicalString());
        }
    }

    private void startServer() {
        port(httpPort);
        staticFileLocation("/demoServerHtml");
        webSocket("/messages", NotificationsWebSocket.class);
        LOG.info("Starting server on port " + httpPort + "....");
        init();
    }

    /**
     * Start the server. Registers in Cloudname, starts a heartbeat thread for the clients connected
     * via web sockets and starts monitoring for services.
     */
    public static void main(final String[] args) {
        new Flags().loadOpts(PacServer.class).parse(args);

        final PacServer demoServer = new PacServer();

        demoServer.connectToCloudname();

        // Start publishing a heartbeat
        Executors.newSingleThreadExecutor().execute(() -> {
            while (true) {
                try {
                    Thread.sleep(10000L);
                    publisher.publish(new JSONObject()
                            .put("action", "heartbeat")
                            .put("time", System.currentTimeMillis())
                            .toString());
                } catch (final InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            }
        });

        demoServer.startServer();

    }
}
