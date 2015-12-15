package org.cloudname.samples.service;

import org.cloudname.core.BackendManager;
import org.cloudname.flags.Flag;
import org.cloudname.flags.Flags;

import org.cloudname.service.CloudnameService;
import org.cloudname.service.Endpoint;
import org.cloudname.service.InstanceCoordinate;
import org.cloudname.service.ServiceCoordinate;
import org.cloudname.service.ServiceData;
import org.cloudname.service.ServiceListener;
import org.json.JSONObject;

import static spark.Spark.*;

/**
 * The server hosting the web page. Static pages (on the root) are served out of the resources,
 * web socket is served through /messages.
 */
public class DemoServer {
    @Flag (name = "cloudname-url", description = "Cloudname URL", required = true)
    private static String cloudnameUrl = null;

    @Flag (name = "coordinate", description = "Service coordinate", required = false)
    private static String myCoordinate = "pacman.test.local";

    private final CloudnameService service;

    public static final NotificationPublisher publisher = new NotificationPublisher();

    private DemoServer() {
        service = new CloudnameService(BackendManager.getBackend(cloudnameUrl));
    }

    private String getCreateNotification(final InstanceCoordinate coordinate, final ServiceData serviceData) {
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
        final String[] ghostNames = new String[] { "pinky", "blinky", "inky", "clyde" };
        for (final String name : ghostNames) {
            final ServiceCoordinate ghostCoordinate = new ServiceCoordinate.Builder()
                    .fromCoordinate(ServiceCoordinate.parse(myCoordinate))
                    .setService(name)
                    .build();

            service.addServiceListener(ghostCoordinate, new ServiceListener() {
                @Override
                public void onServiceCreated(InstanceCoordinate coordinate, ServiceData serviceData) {
                    publisher.publish(getCreateNotification(coordinate, serviceData));
                }

                @Override
                public void onServiceDataChanged(InstanceCoordinate coordinate, ServiceData data) {
                    // ignore
                }

                @Override
                public void onServiceRemoved(InstanceCoordinate coordinate) {
                    publisher.publish(getRemoveNotification(coordinate));
                }
            });
        }
    }

    private void startServer() {
        staticFileLocation("/demoServerHtml");
        webSocket("/messages", NotificationsWebSocket.class);
        System.out.println("Starting server....");
        init();
    }
    public static void main(final String[] args) {
        new Flags().loadOpts(DemoServer.class).parse(args);

        final DemoServer demoServer = new DemoServer();
        demoServer.connectToCloudname();
        demoServer.startServer();
    }
}
