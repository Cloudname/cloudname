package org.cloudname.service;

import org.cloudname.backends.memory.MemoryBackend;
import org.cloudname.core.BackendManager;
import org.cloudname.core.CloudnameBackend;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;

/**
 * Test service registration with memory-based backend.
 */
public class CloudnameServiceTest {
    private static final CloudnameBackend memoryBackend = BackendManager.getBackend("memory://");

    private final ServiceCoordinate coordinate = ServiceCoordinate.parse("service.tag.region");

    /**
     * Max time to wait for changes to propagate to clients. In seconds.
     */
    private static final int MAX_WAIT_S = 1;

    private final Random random = new Random();
    private int getRandomPort() {
        return Math.max(1, Math.abs(random.nextInt(4096)));
    }

    private ServiceHandle registerService(final CloudnameService cloudnameService, final String serviceCoordinateString) {
        final ServiceCoordinate serviceCoordinate = ServiceCoordinate.parse(serviceCoordinateString);

        final Endpoint httpEndpoint = new Endpoint("http", "127.0.0.1", getRandomPort());
        final Endpoint webconsoleEndpoint = new Endpoint("webconsole", "127.0.0.2", getRandomPort());

        final ServiceData serviceData = new ServiceData(Arrays.asList(httpEndpoint, webconsoleEndpoint));
        return cloudnameService.registerService(serviceCoordinate, serviceData);
    }

    /**
     * Create two sets of services, register both and check that notifications are sent to the
     * subscribers.
     */
    @Test
    public void testServiceNotifications() throws InterruptedException {
        final String SOME_COORDINATE = "someservice.test.local";
        final String ANOTHER_COORDINATE = "anotherservice.test.local";

        final CloudnameService mainCloudname = new CloudnameService(memoryBackend);

        final int numOtherServices = 10;
        final List<ServiceHandle> handles = new ArrayList<>();
        for (int i = 0; i < numOtherServices; i++) {
            handles.add(registerService(mainCloudname, ANOTHER_COORDINATE));
        }

        final Executor executor = Executors.newCachedThreadPool();
        final int numServices = 5;
        final CountDownLatch registrationLatch = new CountDownLatch(numServices);
        final CountDownLatch instanceLatch = new CountDownLatch(numServices * numOtherServices);
        final CountDownLatch httpEndpointLatch = new CountDownLatch(numServices * numOtherServices);
        final CountDownLatch webconsoleEndpointLatch = new CountDownLatch(numServices * numOtherServices);
        final CountDownLatch removeLatch = new CountDownLatch(numServices * numOtherServices);
        final Semaphore terminateSemaphore = new Semaphore(1);
        final CountDownLatch completedLatch = new CountDownLatch(numServices);

        final Runnable service = new Runnable() {
            @Override
            public void run() {
                try (final CloudnameService cloudnameService = new CloudnameService(memoryBackend)) {
                    try (final ServiceHandle handle = registerService(cloudnameService, SOME_COORDINATE)) {
                        registrationLatch.countDown();

                        final ServiceCoordinate otherServiceCoordinate = ServiceCoordinate.parse(ANOTHER_COORDINATE);

                        // Do a service lookup on the other service. This will yield N elements.
                        cloudnameService.addServiceListener(otherServiceCoordinate, new ServiceListener() {
                            @Override
                            public void onServiceCreated(final InstanceCoordinate coordinate, final ServiceData data) {
                                instanceLatch.countDown();
                                if (data.getEndpoint("http") != null) {
                                    httpEndpointLatch.countDown();
                                }
                                if (data.getEndpoint("webconsole") != null) {
                                    webconsoleEndpointLatch.countDown();
                                }
                            }

                            @Override
                            public void onServiceDataChanged(final InstanceCoordinate coordinate, final ServiceData data) {
                                if (data.getEndpoint("http") != null) {
                                    httpEndpointLatch.countDown();
                                }
                                if (data.getEndpoint("webconsole") != null) {
                                    webconsoleEndpointLatch.countDown();
                                }
                            }

                            @Override
                            public void onServiceRemoved(final InstanceCoordinate coordinate) {
                                removeLatch.countDown();
                            }
                        });

                        // Wait for the go ahead before terminating
                        try {
                            terminateSemaphore.acquire();
                            terminateSemaphore.release();
                        } catch (final InterruptedException ie) {
                            throw new RuntimeException(ie);
                        }
                    }
                    // The service handle will close and the instance will be removed at this point.
                }
                completedLatch.countDown();
            }
        };

        // Grab the semaphore. This wil stop the services from terminating
        terminateSemaphore.acquire();

        // Start two threads which will register a service and look up a set of another.
        for (int i = 0; i < numServices; i++) {
            executor.execute(service);
        }

        // Wait for the registrations and endpoints to propagate
        assertTrue("Expected registrations to complete",
                registrationLatch.await(MAX_WAIT_S, TimeUnit.SECONDS));

        assertTrue("Expected http endpoints to be registered but missing "
                + httpEndpointLatch.getCount(),
                httpEndpointLatch.await(MAX_WAIT_S, TimeUnit.SECONDS));

        assertTrue("Expected webconsole endpoints to be registered but missing "
                + webconsoleEndpointLatch.getCount(),
                webconsoleEndpointLatch.await(MAX_WAIT_S, TimeUnit.SECONDS));

        // Registrations are now completed; remove the existing services
        for (final ServiceHandle handle : handles) {
            handle.close();
        }

        // This will trigger remove events in the threads.
        assertTrue("Expected services to be removed but " + removeLatch.getCount()
                + " still remains", removeLatch.await(MAX_WAIT_S, TimeUnit.SECONDS));

        // Let the threads terminate. This will remove the registrations
        terminateSemaphore.release();

        assertTrue("Expected services to complete but " + completedLatch.getCount()
                        + " still remains", completedLatch.await(MAX_WAIT_S, TimeUnit.SECONDS));

        // Success! There shouldn't be any more services registered at this point. Check to make sure
        mainCloudname.addServiceListener(ServiceCoordinate.parse(SOME_COORDINATE), new ServiceListener() {
            @Override
            public void onServiceCreated(final InstanceCoordinate coordinate, final ServiceData data) {
                fail("Should not have any services but " + coordinate + " is still there");
            }

            @Override
            public void onServiceDataChanged(final InstanceCoordinate coordinate, final ServiceData data) {
                fail("Should not have any services but " + coordinate + " reports data");
            }

            @Override
            public void onServiceRemoved(final InstanceCoordinate coordinate) {

            }
        });
        mainCloudname.addServiceListener(ServiceCoordinate.parse(ANOTHER_COORDINATE), new ServiceListener() {
            @Override
            public void onServiceCreated(final InstanceCoordinate coordinate, final ServiceData data) {
                fail("Should not have any services but " + coordinate + " is still there");
            }

            @Override
            public void onServiceDataChanged(final InstanceCoordinate coordinate, final ServiceData data) {
                fail("Should not have any services but " + coordinate + " is still there");
            }

            @Override
            public void onServiceRemoved(InstanceCoordinate coordinate) {

            }
        });
    }

    /**
     * Ensure data notifications works as expecte. Update a lot of endpoints on a single
     * service and check that the subscribers get notified of all changes in the correct order.
     */
    @Test
    public void testDataNotifications() throws InterruptedException {
        final CloudnameService cs = new CloudnameService(memoryBackend);

        final String serviceCoordinate = "some.service.name";
        final ServiceHandle serviceHandle = cs.registerService(
                ServiceCoordinate.parse(serviceCoordinate),
                new ServiceData(new ArrayList<Endpoint>()));

        final int numClients = 10;
        final int numDataChanges = 50;
        final int maxSecondsForNotifications = 1;
        final CountDownLatch dataChangeLatch = new CountDownLatch(numClients * numDataChanges);
        final CountDownLatch readyLatch = new CountDownLatch(numClients);
        final String EP_NAME = "endpoint";
        final Semaphore terminateSemaphore = new Semaphore(1);

        // Grab the semaphore, prevent threads from completing
        terminateSemaphore.acquire();

        final Runnable clientServices = new Runnable() {
            @Override
            public void run() {
                try (final CloudnameService cn = new CloudnameService(memoryBackend)) {
                    cn.addServiceListener(ServiceCoordinate.parse(serviceCoordinate), new ServiceListener() {
                        int portNum = 0;
                        @Override
                        public void onServiceCreated(InstanceCoordinate coordinate, ServiceData serviceData) {
                            // ignore this
                        }

                        @Override
                        public void onServiceDataChanged(InstanceCoordinate coordinate, ServiceData data) {
                            final Endpoint ep = data.getEndpoint(EP_NAME);
                            if (ep != null) {
                                dataChangeLatch.countDown();
                                assertThat(ep.getPort(), is(portNum + 1));
                                portNum = portNum + 1;
                            }
                        }

                        @Override
                        public void onServiceRemoved(InstanceCoordinate coordinate) {
                            // ignore this
                        }
                    });
                    readyLatch.countDown();

                    // Wait for the test to finish before closing. The endpoints will be
                    // processed once every thread is ready.
                    try {
                        terminateSemaphore.acquire();
                        terminateSemaphore.release();
                    } catch (final InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }
                }
            }
        };

        final Executor executor = Executors.newCachedThreadPool();
        for (int i = 0; i < numClients; i++) {
            executor.execute(clientServices);
        }

        // Wait for the threads to be ready
        readyLatch.await();

        // Publish changes to the same endpoint; the endpoint is updated with a new port
        // number for each update.
        Endpoint oldEndpoint = null;
        for (int portNum = 1; portNum < numDataChanges + 1; portNum++) {
            if (oldEndpoint != null) {
                serviceHandle.removeEndpoint(oldEndpoint);
            }
            final Endpoint newEndpoint = new Endpoint(EP_NAME, "localhost", portNum);
            serviceHandle.registerEndpoint(newEndpoint);
            oldEndpoint = newEndpoint;
        }

        // Check if the threads have been notified of all the changes
        assertTrue("Expected " + (numDataChanges * numClients) + " changes but "
                + dataChangeLatch.getCount() + " remains",
                dataChangeLatch.await(maxSecondsForNotifications, TimeUnit.SECONDS));

        // Let threads terminate
        terminateSemaphore.release();
    }

    @Test(expected = IllegalArgumentException.class)
    public void coordinateCanNotBeNullWhenAddingListener() {
        new CloudnameService(memoryBackend).addServiceListener(null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void listenerCanNotBeNullWhenAddingListener() {
        new CloudnameService(memoryBackend).addServiceListener(coordinate, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void serviceCannotBeNullWhenRegister() {
        new CloudnameService(memoryBackend).registerService(null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void serviceDataCannotBeNullWhenRegister() {
        new CloudnameService(memoryBackend).registerService(coordinate, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void backendMustBeValid() {
        new CloudnameService(null);
    }
}
