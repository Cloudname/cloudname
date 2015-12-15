package org.cloudname.samples.service;

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
import org.cloudname.testtools.Net;
import spark.Spark;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * A sample service. This service provides a simple web page with a coordinate, known peers and a
 * shutdown button.
 *
 * @author stalehd@gmail.com
 */
public class GhostService {
    private static final Logger LOG = Logger.getLogger(GhostService.class.getName());

    @Flag (name = "cloudname-url", description = "Cloudname URL", required = true)
    private static String cloudnameUrl = null;

    @Flag(name = "service-name", description = "Service name", required = false)
    private static String myServiceName = null;

    @Flag (name = "coordinate", description = "Service coordinate", required = false)
    private static String myCoordinate = "blinky.test.local";

    private final CountDownLatch terminateLatch = new CountDownLatch(1);
    private final AtomicReference<InstanceCoordinate> assignedCoordinate = new AtomicReference<>();
    private final Map<String, InstanceCoordinate> peerMap = new ConcurrentHashMap<>();
    private final CloudnameService service;
    private final int sparkPort;

    private GhostService() {
        service = new CloudnameService(BackendManager.getBackend(cloudnameUrl));
        try {
            sparkPort = Net.getFreePort();
        } catch (final IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private void connectToCloudname() {
        final ServiceData serviceData = new ServiceData();
        for (final String endpoint : Net.getHostInterfaces()) {
            serviceData.addEndpoint(new Endpoint("http", endpoint, sparkPort));
        }
        try (final ServiceHandle handle
                     = service.registerService(getServiceCoordinate(), serviceData)) {
            assignedCoordinate.set(handle.getCoordinate());
            service.addServiceListener(getServiceCoordinate(), new ServiceListener() {
                @Override
                public void onServiceCreated(
                        final InstanceCoordinate coordinate, final ServiceData serviceData) {
                    LOG.info("Adding " + coordinate + " to peer set");
                    peerMap.put(coordinate.toCanonicalString(), coordinate);
                }

                @Override
                public void onServiceDataChanged(
                        final InstanceCoordinate coordinate, final ServiceData data) {

                }

                @Override
                public void onServiceRemoved(final InstanceCoordinate coordinate) {
                    LOG.info("Removing " + coordinate + " from peer set");
                    peerMap.remove(coordinate.toCanonicalString());
                }
            });
            try {
                terminateLatch.await();
                LOG.info("Stopping service");
                Spark.stop();
            } catch (final InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }
        LOG.info("Closed connection");
    }

    private ServiceCoordinate getServiceCoordinate() {
        if (myServiceName != null) {
            return new ServiceCoordinate.Builder()
                    .fromCoordinate(ServiceCoordinate.parse(myCoordinate))
                    .setService(myServiceName)
                    .build();
        }
        return ServiceCoordinate.parse(myCoordinate);
    }

    private String getPeerSetList() {
        final StringBuilder sb = new StringBuilder().append("<ul>");
        for (final InstanceCoordinate coordinate : peerMap.values()) {
            sb.append("<li>").append(coordinate.toCanonicalString()).append("</li>");
        }
        sb.append("</ul>");
        return sb.toString();
    }

    private String getStatusPage() {
        return  "<html lang=\"en\">\n"
                + "    <head>\n"
                + "        <title>Hello there!</title>\n"
                + "        <style>\n"
                + "            body { \n"
                + "                background-color: black; \n"
                + "                font-family: sans-serif; \n"
                + "                font-size: 14pt; \n"
                + "                color: lightblue;\n"
                + "            }\n"
                + "        </style>\n"
                + "    </head>\n"
                + "    <body>\n"
                + "        <p>My coordinate is <strong>"
                + assignedCoordinate.get().toCanonicalString()
                + "</strong></p>\n"
                + "        <h1>My peers</h1>\n"
                + getPeerSetList()
                + "        <hr/>\n"
                + "        <form action=\"/shutdown\" method=\"POST\">\n"
                + "            <button type=\"submit\">Shut down</button>\n"
                + "        </form>\n"
                + "    </body>\n"
                + "</html>";
    }

    private void setupWebserver() {
        Spark.port(sparkPort);
        Spark.get("/", (req, res) ->  getStatusPage());
        Spark.post("/shutdown", (req, res) -> {
            terminateLatch.countDown();
            return "<strong>Shutting down in a jiffy!</strong>"
                    + "<script>setTimeout(function() { window.close(); }, 1000);</script>";
        });
        Spark.init();
    }

    /**
     * Launch the test server.
     */
    public static void main(final String[] args) {
        new Flags().loadOpts(GhostService.class).parse(args);
        final GhostService service = new GhostService();
        Executors.newSingleThreadExecutor().execute(() -> {
            Spark.awaitInitialization();
            service.connectToCloudname();
        });
        service.setupWebserver();
    }
}
