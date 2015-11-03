package org.cloudname.service;

import org.cloudname.backends.memory.MemoryBackend;
import org.cloudname.core.CloudnameBackend;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test persistent services functions.
 */
public class CloudnameServicePermanentTest {
    private static final String SERVICE_COORDINATE = "myoldskoolserver.test.local";
    private static final CloudnameBackend memoryBackend = new MemoryBackend();
    private static final Endpoint DEFAULT_ENDPOINT = new Endpoint("serviceport", "localhost", 80);
    private final ServiceCoordinate serviceCoordinate = ServiceCoordinate.parse(SERVICE_COORDINATE);

    @BeforeClass
    public static void createServiceRegistration() {
        try (final CloudnameService cloudnameService = new CloudnameService(memoryBackend)) {
            assertThat(
                    cloudnameService.createPermanentService(
                            ServiceCoordinate.parse(SERVICE_COORDINATE), DEFAULT_ENDPOINT),
                    is(true));
        }
    }

    @Test
    public void testPersistentServiceChanges() throws InterruptedException {
        try (final CloudnameService cloudnameService = new CloudnameService(memoryBackend)) {

            final CountDownLatch callCounter = new CountDownLatch(2);
            final int secondsToWait = 1;

            // ...a listener on the service will trigger when there's a change plus the initial
            // onCreate call.
            cloudnameService.addPermanentServiceListener(serviceCoordinate,
                    new PermanentServiceListener() {
                        private final AtomicInteger createCount = new AtomicInteger(0);
                        private final AtomicInteger changeCount = new AtomicInteger(0);

                        @Override
                        public void onServiceCreated(Endpoint endpoint) {
                            // Expect this to be called once and only once, even on updates
                            assertThat(createCount.incrementAndGet(), is(1));
                            callCounter.countDown();
                        }

                        @Override
                        public void onServiceChanged(Endpoint endpoint) {
                            // This will be called when the endpoint changes
                            assertThat(changeCount.incrementAndGet(), is(1));
                            callCounter.countDown();
                        }

                        @Override
                        public void onServiceRemoved() {
                            // This won't be called
                            fail("Did not expect onServiceRemoved to be called");
                        }
                    });

            // Updating with invalid endpoint name fails
            assertThat(cloudnameService.updatePermanentService(serviceCoordinate,
                            new Endpoint("wrongep", DEFAULT_ENDPOINT.getHost(), 81)),
                    is(false));

            // Using the right one, however, does work
            assertThat(cloudnameService.updatePermanentService(serviceCoordinate,
                            new Endpoint(
                                    DEFAULT_ENDPOINT.getName(), DEFAULT_ENDPOINT.getHost(), 81)),
                    is(true));
            // Wait for notifications
            callCounter.await(secondsToWait, TimeUnit.SECONDS);

        }

        // At this point the service created above is closed; changes to the service won't
        // trigger errors in the listener declared. Just do one change to make sure.
        final CloudnameService cloudnameService = new CloudnameService(memoryBackend);
        assertThat(cloudnameService.updatePermanentService(
                ServiceCoordinate.parse(SERVICE_COORDINATE), DEFAULT_ENDPOINT), is(true));
    }

    @Test
    public void testDuplicateRegistration() {
        try (final CloudnameService cloudnameService = new CloudnameService(memoryBackend)) {
            // Creating the same permanent service will fail
            assertThat("Can't create two identical permanent services",
                    cloudnameService.createPermanentService(serviceCoordinate, DEFAULT_ENDPOINT),
                    is(false));
        }
    }

    @Test (expected = IllegalArgumentException.class)
    public void testNullCoordinateRegistration() {
        try (final CloudnameService cloudnameService = new CloudnameService(memoryBackend)) {
           cloudnameService.createPermanentService(null, DEFAULT_ENDPOINT);
        }
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidEndpoint() {
        try (final CloudnameService cloudnameService = new CloudnameService(memoryBackend)) {
            cloudnameService.createPermanentService(serviceCoordinate, null);
        }
    }

    @Test
    public void testListenerOnServiceThatDoesntExist() throws InterruptedException {
        final String anotherServiceCoordinate = "someother.service.coordinate";

        // It should be possible to listen for a permanent service that doesn't exist yet. Once the
        // service is created it must trigger a callback to the clients listening.
        try (final CloudnameService cloudnameService = new CloudnameService(memoryBackend)) {

            final CountDownLatch createCalls = new CountDownLatch(1);
            final CountDownLatch removeCalls = new CountDownLatch(1);
            final CountDownLatch updateCalls = new CountDownLatch(1);

            cloudnameService.addPermanentServiceListener(
                    ServiceCoordinate.parse(anotherServiceCoordinate),
                    new PermanentServiceListener() {
                        final AtomicInteger order = new AtomicInteger(0);
                        @Override
                        public void onServiceCreated(Endpoint endpoint) {
                            createCalls.countDown();
                            assertThat(order.incrementAndGet(), is(1));
                        }

                        @Override
                        public void onServiceChanged(Endpoint endpoint) {
                            updateCalls.countDown();
                            assertThat(order.incrementAndGet(), is(2));
                        }

                        @Override
                        public void onServiceRemoved() {
                            removeCalls.countDown();
                            assertThat(order.incrementAndGet(), is(3));
                        }
                    });

            // Create the new service registration, change the endpoint, then remove it. The
            // count down latches should count down and the order should be create, change, remove
            final ServiceCoordinate another = ServiceCoordinate.parse(anotherServiceCoordinate);
            cloudnameService.createPermanentService(another, DEFAULT_ENDPOINT);
            cloudnameService.updatePermanentService(another,
                    new Endpoint(DEFAULT_ENDPOINT.getName(), "otherhost", 4711));
            cloudnameService.removePermanentService(another);

            final int secondsToWait = 1;
            assertTrue("Expected callback for create to trigger but it didn't",
                    createCalls.await(secondsToWait, TimeUnit.SECONDS));
            assertTrue("Expected callback for update to trigger but it didn't",
                    updateCalls.await(secondsToWait, TimeUnit.SECONDS));
            assertTrue("Expected callback for remove to trigger but it didn't",
                    removeCalls.await(secondsToWait, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testLeaseUpdateOnLeaseThatDoesntExist() {
        try (final CloudnameService cloudnameService = new CloudnameService(memoryBackend)) {
            assertThat("Can't update a service that doesn't exist",
                    cloudnameService.updatePermanentService(
                            ServiceCoordinate.parse("foo.bar.baz"), DEFAULT_ENDPOINT),
                    is(false));
        }
    }

    @Test
    public void testRemoveServiceThatDoesntExist() {
        try (final CloudnameService cloudnameService = new CloudnameService(memoryBackend)) {
            assertThat("Can't remove a service that doesn't exist",
                    cloudnameService.removePermanentService(ServiceCoordinate.parse("foo.bar.baz")),
                    is(false));
        }
    }

    @AfterClass
    public static void removeServiceRegistration() throws InterruptedException {
        try (final CloudnameService cloudnameService = new CloudnameService(memoryBackend)) {
            final ServiceCoordinate serviceCoordinate = ServiceCoordinate.parse(SERVICE_COORDINATE);
            final CountDownLatch callCounter = new CountDownLatch(2);
            final int secondsToWait = 1;
            cloudnameService.addPermanentServiceListener(serviceCoordinate,
                    new PermanentServiceListener() {
                        private final AtomicInteger createCount = new AtomicInteger(0);
                        private final AtomicInteger removeCount = new AtomicInteger(0);

                        @Override
                        public void onServiceCreated(final Endpoint endpoint) {
                            // This will be called once and only once
                            assertThat("Did not onServiceCreated to be called multiple times",
                                    createCount.incrementAndGet(), is(1));
                            callCounter.countDown();
                        }

                        @Override
                        public void onServiceChanged(final Endpoint endpoint) {
                            fail("Did not expect any calls to onServiceChanged");
                        }

                        @Override
                        public void onServiceRemoved() {
                            assertThat("Did not expect onServiceRemoved to be called multiple"
                                    + " times", removeCount.incrementAndGet(), is(1));
                            callCounter.countDown();
                        }
                    });

            // Remove the service created in the setup.
            assertThat(cloudnameService.removePermanentService(serviceCoordinate), is(true));

            assertTrue("Did not receive the expected number of calls to listener. "
                    + callCounter.getCount() + " calls remaining.",
                    callCounter.await(secondsToWait, TimeUnit.SECONDS));

            // Removing it twice will fail.
            assertThat(cloudnameService.removePermanentService(serviceCoordinate), is(false));
        }
    }

    private final ServiceCoordinate coordinate = ServiceCoordinate.parse("service.tag.region");

    @Test (expected = IllegalArgumentException.class)
    public void coordinateCanNotBeNullWhenUpdatingService() {
        new CloudnameService(memoryBackend).updatePermanentService(null, null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void endpointCanNotBeNullWhenUpdatingService() {
        new CloudnameService(memoryBackend).updatePermanentService(coordinate, null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void coordinateCanNotBeNullWhenRemovingService() {
        new CloudnameService(memoryBackend).removePermanentService(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void coordinateCanNotBeNullWhenAddingListener() {
        new CloudnameService(memoryBackend).addPermanentServiceListener(null, null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void listenerCanNotBeNullWhenAddingListener() {
        new CloudnameService(memoryBackend).addPermanentServiceListener(coordinate, null);
    }

}
