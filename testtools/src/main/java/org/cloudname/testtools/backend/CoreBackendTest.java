package org.cloudname.testtools.backend;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.cloudname.core.CloudnameBackend;
import org.cloudname.core.CloudnamePath;
import org.cloudname.core.LeaseHandle;
import org.cloudname.core.LeaseListener;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Core backend tests. This ensures the backend implementation works as expected on the most
 * basic level. Override this class in your backend implementation to test it.
 *
 * @author stalehd@gmail.com
 */
public abstract class CoreBackendTest {
    private final CloudnamePath serviceA = new CloudnamePath(
            new String[] {"local", "test", "service-a"});
    private final CloudnamePath serviceB = new CloudnamePath(
            new String[] {"local", "test", "service-b"});

    private final Random random = new Random();

    /**
     * Max data propagation time (in ms) for notifications from the backend. Override if your
     * backend implementation is slow. 100 ms is a lot of time though so do it carefully.
     */
    protected int getBackendPropagationTime() {
        return 100;
    }
    /**
     * Ensure multiple clients can connect and that leases get an unique path for each client.
     */
    @Test
    public void temporaryLeaseCreation() throws Exception {
        try (final CloudnameBackend backend = getBackend()) {
            final String data = Long.toHexString(random.nextLong());
            final LeaseHandle lease = backend.createTemporaryLease(serviceA, data);
            assertThat("Expected lease to be not null", lease, is(notNullValue()));

            assertTrue("Expected lease path to be a subpath of the supplied lease (" + serviceA
                            + ") but it is " + lease.getLeasePath(),
                    serviceA.isSubpathOf(lease.getLeasePath()));

            assertThat("The temporary lease data can be read",
                    backend.readTemporaryLeaseData(lease.getLeasePath()), is(data));

            final String newData = Long.toHexString(random.nextLong());
            assertThat("Expected to be able to write lease data but didn't",
                    lease.writeLeaseData(newData), is(true));

            assertThat("Expected to be able to read data back but didn't",
                    backend.readTemporaryLeaseData(lease.getLeasePath()), is(newData));
            lease.close();

            assertThat("Expect the lease path to be null", lease.getLeasePath(), is(nullValue()));

            assertFalse("Did not expect to be able to write lease data for a closed lease",
                    lease.writeLeaseData(Long.toHexString(random.nextLong())));
            assertThat("The temporary lease data can not be read",
                    backend.readTemporaryLeaseData(lease.getLeasePath()), is(nullValue()));


            final int numberOfLeases = 50;

            final Set<String> leasePaths = new HashSet<>();
            for (int i = 0; i < numberOfLeases; i++) {
                final String randomData = Long.toHexString(random.nextLong());
                final LeaseHandle handle = backend.createTemporaryLease(serviceB, randomData);
                leasePaths.add(handle.getLeasePath().join(':'));
                handle.close();
            }

            assertThat("Expected " + numberOfLeases + " unique paths but it was "
                    + leasePaths.size(),
                    leasePaths.size(), is(numberOfLeases));
        }
    }

    /**
     * A very simple single-threaded notification. Make sure this works before implementing
     * the multiple notifications elsewhere in this test.
     */
    @Test
    public void simpleTemporaryNotification() throws Exception {

        try (final CloudnameBackend backend = getBackend()) {

            final CloudnamePath rootPath = new CloudnamePath(new String[]{"simple"});
            final CountDownLatch createCounter = new CountDownLatch(1);
            final CountDownLatch removeCounter = new CountDownLatch(1);
            final CountDownLatch dataCounter = new CountDownLatch(1);

            final String firstData = "first data";
            final String lastData = "last data";
            final LeaseListener listener = new LeaseListener() {
                @Override
                public void leaseCreated(final CloudnamePath path, final String data) {
                    createCounter.countDown();
                    if (data.equals(lastData)) {
                        dataCounter.countDown();
                    }
                }

                @Override
                public void leaseRemoved(final CloudnamePath path) {
                    removeCounter.countDown();
                }

                @Override
                public void dataChanged(final CloudnamePath path, final String data) {
                    dataCounter.countDown();
                }
            };
            backend.addTemporaryLeaseListener(rootPath, listener);
            final LeaseHandle handle = backend.createTemporaryLease(rootPath, firstData);
            assertThat(handle, is(notNullValue()));
            Thread.sleep(getBackendPropagationTime());

            handle.writeLeaseData(lastData);
            Thread.sleep(getBackendPropagationTime());

            handle.close();

            assertTrue("Expected create notification but didn't get one",
                    createCounter.await(getBackendPropagationTime(), TimeUnit.MILLISECONDS));
            assertTrue("Expected remove notification but didn't get one",
                    removeCounter.await(getBackendPropagationTime(), TimeUnit.MILLISECONDS));
            assertTrue("Expected data notification but didn't get one",
                    dataCounter.await(getBackendPropagationTime(), TimeUnit.MILLISECONDS));

            backend.removeTemporaryLeaseListener(listener);
        }
    }

    /**
     * Ensure permanent leases can be created and that they can't be overwritten by clients using
     * the library.
     */
    @Test
    public void permanentLeaseCreation() throws Exception {
        final CloudnamePath leasePath = new CloudnamePath(new String[]{"some", "path"});
        final String dataString = "some data string";
        final String newDataString = "new data string";


        try (final CloudnameBackend backend = getBackend()) {
            backend.removePermanentLease(leasePath);

            assertThat("Permanent lease can be created",
                    backend.createPermanantLease(leasePath, dataString), is(true));

            assertThat("Permanent lease data can be read",
                    backend.readPermanentLeaseData(leasePath), is(dataString));

            assertThat("Permanent lease can't be created twice",
                    backend.createPermanantLease(leasePath, dataString), is(false));

            assertThat("Permanent lease can be updated",
                    backend.writePermanentLeaseData(leasePath, newDataString), is(true));

            assertThat("Permanent lease data can be read after update",
                    backend.readPermanentLeaseData(leasePath), is(newDataString));
        }

        try (final CloudnameBackend backend = getBackend()) {
            assertThat("Permanent lease data can be read from another backend",
                    backend.readPermanentLeaseData(leasePath), is(newDataString));
            assertThat("Permanent lease can be removed",
                    backend.removePermanentLease(leasePath), is(true));
            assertThat("Lease can't be removed twice",
                    backend.removePermanentLease(leasePath), is(false));
            assertThat("Lease data can't be read from deleted lease",
                    backend.readPermanentLeaseData(leasePath), is(nullValue()));
        }
    }

    /**
     * Ensure clients are notified of changes.
     */
    @Test
    public void multipleTemporaryNotifications() throws Exception {
        try (final CloudnameBackend backend = getBackend()) {
            final CloudnamePath rootPath = new CloudnamePath(new String[]{"root", "lease"});
            final String clientData = "client data here";

            final LeaseHandle lease = backend.createTemporaryLease(rootPath, clientData);
            assertThat("Handle to lease is returned", lease, is(notNullValue()));
            assertThat("Lease is a child of the root lease",
                    rootPath.isSubpathOf(lease.getLeasePath()), is(true));

            int numListeners = 10;
            final int numUpdates = 10;

            // Add some listeners to the temporary lease. Each should be notified once on
            // creation, once on removal and once every time the data is updated
            final CountDownLatch createNotifications = new CountDownLatch(numListeners);
            final CountDownLatch dataNotifications = new CountDownLatch(numListeners * numUpdates);
            final CountDownLatch removeNotifications = new CountDownLatch(numListeners);

            final List<LeaseListener> listeners = new ArrayList<>();
            for (int i = 0; i < numListeners; i++) {
                final LeaseListener listener = new LeaseListener() {
                    private AtomicInteger lastData = new AtomicInteger(-1);

                    @Override
                    public void leaseCreated(final CloudnamePath path, final String data) {
                        createNotifications.countDown();
                    }

                    @Override
                    public void leaseRemoved(final CloudnamePath path) {
                        removeNotifications.countDown();
                    }

                    @Override
                    public void dataChanged(final CloudnamePath path, final String data) {
                        assertThat(lastData.incrementAndGet(), is(Integer.parseInt(data)));
                        dataNotifications.countDown();
                    }
                };
                listeners.add(listener);
                backend.addTemporaryLeaseListener(rootPath, listener);
            }

            // Change the data a few times. Every change should be propagated to the listeners
            // in the same order they have changed
            for (int i = 0; i < numUpdates; i++) {
                lease.writeLeaseData(Integer.toString(i));
                Thread.sleep(getBackendPropagationTime());
            }

            // Remove the lease. Removal notifications will be sent to the clients

            assertThat("All create notifications are received but " + createNotifications.getCount()
                            + " remains out of " + numListeners,
                    createNotifications.await(getBackendPropagationTime(), TimeUnit.MICROSECONDS),
                    is(true));

            assertThat("All data notifications are received but " + dataNotifications.getCount()
                            + " remains out of " + (numListeners * numUpdates),
                    dataNotifications.await(getBackendPropagationTime(), TimeUnit.MILLISECONDS),
                    is(true));

            lease.close();
            assertThat("All remove notifications are received but " + removeNotifications.getCount()
                            + " remains out of " + numListeners,
                    removeNotifications.await(getBackendPropagationTime(), TimeUnit.MILLISECONDS),
                    is(true));

            // Remove the listeners
            for (final LeaseListener listener : listeners) {
                lease.close();
                backend.removeTemporaryLeaseListener(listener);
            }
        }
    }

    /**
     * Test a simple peer to peer scheme; all clients grabbing a lease and listening on other
     * clients.
     */
    @Test
    public void multipleServicesWithMultipleClients() throws Exception {
        try (final CloudnameBackend backend = getBackend()) {

            final CloudnamePath rootLease = new CloudnamePath(new String[]{"multi", "multi"});
            final int numberOfClients = 5;

            // All clients will be notified of all other clients (including themselves)
            final CountDownLatch createNotifications
                    = new CountDownLatch(numberOfClients * numberOfClients);
            // All clients will write one change each
            final CountDownLatch dataNotifications = new CountDownLatch(numberOfClients);
            // There will be 99 + 98 + 97 + 96 ... 1 notifications, in all n (n + 1) / 2
            // remove notifications
            final int n = numberOfClients - 1;
            final CountDownLatch removeNotifications = new CountDownLatch(n * (n + 1) / 2);

            final Runnable clientProcess = new Runnable() {
                @Override
                public void run() {
                    final String myData = Long.toHexString(random.nextLong());
                    final LeaseHandle handle = backend.createTemporaryLease(rootLease, myData);
                    assertThat("Got a valid handle back", handle, is(notNullValue()));
                    backend.addTemporaryLeaseListener(rootLease, new LeaseListener() {
                        @Override
                        public void leaseCreated(final CloudnamePath path, final String data) {
                            assertThat("Notification belongs to root path",
                                    rootLease.isSubpathOf(path), is(true));
                            createNotifications.countDown();
                        }

                        @Override
                        public void leaseRemoved(final CloudnamePath path) {
                            removeNotifications.countDown();
                        }

                        @Override
                        public void dataChanged(final CloudnamePath path, final String data) {
                            dataNotifications.countDown();
                        }
                    });

                    try {
                        assertThat(createNotifications.await(
                                getBackendPropagationTime(), TimeUnit.MILLISECONDS),
                                is(true));
                    } catch (InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }

                    // Change the data for my own lease, wait for it to propagate
                    assertThat(handle.writeLeaseData(Long.toHexString(random.nextLong())),
                            is(true));
                    try {
                        Thread.sleep(getBackendPropagationTime());
                    } catch (final InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }

                    try {
                        assertThat(dataNotifications.await(
                                getBackendPropagationTime(), TimeUnit.MILLISECONDS),
                                is(true));
                    } catch (InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }

                    // ..and close my lease
                    try {
                        handle.close();
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            };

            final Executor executor = Executors.newCachedThreadPool();
            for (int i = 0; i < numberOfClients; i++) {
                executor.execute(clientProcess);
            }

            removeNotifications.await(getBackendPropagationTime(), TimeUnit.SECONDS);
        }
    }

    /**
     * Just make sure unknown listeners doesn't throw exceptions.
     */
    @Test
    public void removeInvalidListener() throws Exception {
        try (final CloudnameBackend backend = getBackend()) {
            final LeaseListener unknownnListener = new LeaseListener() {
                @Override
                public void leaseCreated(final CloudnamePath path, final String data) {
                }

                @Override
                public void leaseRemoved(final CloudnamePath path) {
                }

                @Override
                public void dataChanged(final CloudnamePath path, final String data) {
                }
            };
            backend.removeTemporaryLeaseListener(unknownnListener);
        }
    }


    /**
     * Create a whole set of different listener pairs that runs in parallel. They won't
     * receive notifications from any other lease - listener pairs.
     */
    @Test
    public void multipleIndependentListeners() throws Exception {
        try (final CloudnameBackend backend = getBackend()) {
            final int leasePairs = 10;

            class LeaseWorker {
                private final String id;
                private final CloudnamePath rootPath;
                private final LeaseListener listener;
                private final AtomicInteger createNotifications = new AtomicInteger(0);
                private final AtomicInteger dataNotifications = new AtomicInteger(0);
                private LeaseHandle handle;

                LeaseWorker(final String id) {
                    this.id = id;
                    rootPath = new CloudnamePath(new String[]{"pair", id});
                    listener = new LeaseListener() {

                        @Override
                        public void leaseCreated(final CloudnamePath path, final String data) {
                            createNotifications.incrementAndGet();
                        }

                        @Override
                        public void leaseRemoved(final CloudnamePath path) {
                        }

                        @Override
                        public void dataChanged(final CloudnamePath path, final String data) {
                            dataNotifications.incrementAndGet();
                        }
                    };
                }

                public void createLease() {
                    backend.addTemporaryLeaseListener(rootPath, listener);
                    try {
                        Thread.sleep(getBackendPropagationTime());
                    } catch (final InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }
                    handle = backend.createTemporaryLease(rootPath, id);
                }

                public void writeData() {
                    handle.writeLeaseData(id);
                }

                public void checkNumberOfNotifications() {
                    // There will be two notifications; one for this lease, one for the other
                    assertThat("Expected 2 create notifications", createNotifications.get(), is(2));
                    // There will be two notifications; one for this lease, one for the other
                    assertThat("Expected 2 data notifications", dataNotifications.get(), is(2));
                }

                public void closeLease() {
                    try {
                        handle.close();
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }

            final List<LeaseWorker> workers = new ArrayList<>();

            for (int i = 0; i < leasePairs; i++) {
                final String id = Long.toHexString(random.nextLong());
                final LeaseWorker leaseWorker1 = new LeaseWorker(id);
                leaseWorker1.createLease();
                workers.add(leaseWorker1);
                final LeaseWorker leaseWorker2 = new LeaseWorker(id);
                leaseWorker2.createLease();
                workers.add(leaseWorker2);
            }

            for (final LeaseWorker worker : workers) {
                worker.writeData();
            }
            Thread.sleep(getBackendPropagationTime());
            for (final LeaseWorker worker : workers) {
                worker.checkNumberOfNotifications();
            }
            for (final LeaseWorker worker : workers) {
                worker.closeLease();
            }
        }
    }

    /**
     * Ensure permanent leases distribute notifications as well.
     */
    @Test
    public void permanentLeaseNotifications() throws Exception {
        final CloudnamePath rootLease = new CloudnamePath(new String[] {"permanent", "vacation"});
        final String leaseData = "the aero smiths";
        final String newLeaseData = "popcultural reference";

        try (final CloudnameBackend backend = getBackend()) {
            backend.removePermanentLease(rootLease);
            assertThat("Can create permanent node",
                    backend.createPermanantLease(rootLease, leaseData), is(true));
        }

        final AtomicInteger numberOfNotifications = new AtomicInteger(0);
        final CountDownLatch createLatch = new CountDownLatch(1);
        final CountDownLatch removeLatch = new CountDownLatch(1);
        final CountDownLatch dataLatch = new CountDownLatch(1);

        final LeaseListener listener = new LeaseListener() {
            @Override
            public void leaseCreated(final CloudnamePath path, final String data) {
                assertThat(path, is(equalTo(rootLease)));
                assertThat(data, is(equalTo(leaseData)));
                numberOfNotifications.incrementAndGet();
                createLatch.countDown();
            }

            @Override
            public void leaseRemoved(final CloudnamePath path) {
                assertThat(path, is(equalTo(rootLease)));
                numberOfNotifications.incrementAndGet();
                removeLatch.countDown();
            }

            @Override
            public void dataChanged(final CloudnamePath path, final String data) {
                assertThat(path, is(equalTo(rootLease)));
                assertThat(data, is(equalTo(newLeaseData)));
                numberOfNotifications.incrementAndGet();
                dataLatch.countDown();
            }
        };

        try (final CloudnameBackend backend = getBackend()) {

            assertThat("Lease still exists",
                    backend.readPermanentLeaseData(rootLease), is(leaseData));

            // Add the lease back
            backend.addPermanentLeaseListener(rootLease, listener);

            Thread.sleep(getBackendPropagationTime());

            assertThat("New data can be written",
                    backend.writePermanentLeaseData(rootLease, newLeaseData), is(true));

            Thread.sleep(getBackendPropagationTime());
            // Write new data
            assertThat("Lease can be removed", backend.removePermanentLease(rootLease), is(true));

            assertTrue(createLatch.await(getBackendPropagationTime(), TimeUnit.MILLISECONDS));
            assertTrue(dataLatch.await(getBackendPropagationTime(), TimeUnit.MILLISECONDS));
            assertTrue(removeLatch.await(getBackendPropagationTime(), TimeUnit.MILLISECONDS));
            // This includes one created, one data, one close
            assertThat("One notifications is expected but only got "
                    + numberOfNotifications.get(), numberOfNotifications.get(), is(3));

            backend.removePermanentLeaseListener(listener);
            // just to be sure - this won't upset anything
            backend.removePermanentLeaseListener(listener);
        }
    }


    /**
     * Set up two listeners listening to different permanent leases. There should be no crosstalk
     * between the listeners.
     */
    @Test
    public void multiplePermanentListeners() throws Exception {
        final CloudnamePath permanentA = new CloudnamePath(new String[] {"primary"});
        final CloudnamePath permanentB = new CloudnamePath(new String[] {"secondary"});
        final CloudnamePath permanentC = new CloudnamePath(
                new String[] {"tertiary", "permanent", "lease"});

        try (final CloudnameBackend backend = getBackend()) {
            backend.addPermanentLeaseListener(permanentA, new LeaseListener() {
                @Override
                public void leaseCreated(final CloudnamePath path, final String data) {
                    assertThat(path, is(equalTo(permanentA)));
                }

                @Override
                public void leaseRemoved(final CloudnamePath path) {
                    assertThat(path, is(equalTo(permanentA)));
                }

                @Override
                public void dataChanged(final CloudnamePath path, final String data) {
                    assertThat(path, is(equalTo(permanentA)));
                }
            });

            backend.addPermanentLeaseListener(permanentB, new LeaseListener() {
                @Override
                public void leaseCreated(final CloudnamePath path, final String data) {
                    assertThat(path, is(equalTo(permanentB)));
                }

                @Override
                public void leaseRemoved(final CloudnamePath path) {
                    assertThat(path, is(equalTo(permanentB)));
                }

                @Override
                public void dataChanged(final CloudnamePath path, final String data) {
                    assertThat(path, is(equalTo(permanentB)));
                }
            });

            backend.addPermanentLeaseListener(permanentC, new LeaseListener() {
                @Override
                public void leaseCreated(final CloudnamePath path, final String data) {
                    fail("Did not expect any leases to be created at " + permanentC);
                }

                @Override
                public void leaseRemoved(final CloudnamePath path) {
                    fail("Did not expect any leases to be created at " + permanentC);
                }

                @Override
                public void dataChanged(final CloudnamePath path, final String data) {
                    fail("Did not expect any leases to be created at " + permanentC);
                }
            });

            backend.createPermanantLease(permanentA, "Some data that belongs to A");
            backend.createPermanantLease(permanentB, "Some data that belongs to B");

            // Some might say this is a dirty trick but permanent and temporary leases should not
            // interfere with eachother.
            final LeaseHandle handle = backend.createTemporaryLease(
                    permanentC, "Some data that belongs to C");
            assertThat(handle, is(notNullValue()));
            handle.writeLeaseData("Some other data that belongs to C");
            try {
                handle.close();
            } catch (Exception ex) {
                fail(ex.getMessage());
            }
        }
    }

    protected abstract CloudnameBackend getBackend();
}
