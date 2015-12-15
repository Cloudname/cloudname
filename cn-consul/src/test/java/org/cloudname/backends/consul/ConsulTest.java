package org.cloudname.backends.consul;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.fail;

/**
 * Simple tests for the Consul interface. Needs a running Consul Agent to execute so requires a
 * system property to execute (consul.endpoint). Tests aren't exhaustive.
 *
 * @author Ståle Dahl <stalehd@gmail.com>
 */
public class ConsulTest {
    public static final String EP_PROPERTY = "consul.endpoint";

    private final Random random = new Random();

    private String getSessionName() {
        return "test/consul/session/" + Long.toOctalString(random.nextLong());
    }

    @BeforeClass
    public static void checkIfTestShouldRun() {
        assumeTrue("The system property consul.endpoint isn't set",
                System.getProperty(EP_PROPERTY) != null);
    }

    @Test
    public void testIsValid() {
        final Consul consul = new Consul(System.getProperty(EP_PROPERTY));
        assertTrue(consul.isValid());

        final Consul unavailable = new Consul("http://localhost/");
        assertFalse(unavailable.isValid());
    }

    @Test
    public void createSession() {
        final Consul consul = new Consul(System.getProperty(EP_PROPERTY));
        assertTrue(consul.isValid());

        final ConsulSession session = consul.createSession(getSessionName(), 10, 0);
        assertThat(session, is(notNullValue()));

        assertThat(session.getId(), is(not("")));

        session.close();
    }

    @Test
    public void writeSessionData() throws Exception {
        final Consul consul = new Consul(System.getProperty(EP_PROPERTY));
        assertTrue(consul.isValid());

        final ConsulSession session = consul.createSession(getSessionName(), 10, 0);
        System.out.println(session.getId());
        assertThat(session, is(notNullValue()));
        assertTrue(consul.writeSessionData("TheKey", "TheValue", session.getId()));
        assertThat(consul.readData("TheKey"), is("TheValue"));
        assertTrue(consul.writeSessionData("TheKey", "TheNewValue", session.getId()));
        assertThat(consul.readData("TheKey"), is("TheNewValue"));
        session.close();
    }

    @Test
    public void createPermanentData() {
        final Consul consul = new Consul(System.getProperty(EP_PROPERTY));
        assertTrue(consul.isValid());
        final String keyName = "thepermanentone";
        consul.removePermanentData(keyName);

        assertTrue(consul.createPermanentData(keyName, "SomeValue"));
        assertFalse(consul.createPermanentData(keyName, "AlreadyExists"));
        assertThat(consul.readData(keyName), is("SomeValue"));
        assertTrue(consul.writePermanentData(keyName, "OtherValue"));
        assertThat(consul.readData(keyName), is("OtherValue"));
        assertTrue(consul.removePermanentData(keyName));
    }

    /**
     * Create a session, write a value once, wait a bit, then write again, wait close
     */
    private void createSessionAndWrite(final CountDownLatch readyLatch,
                                       final CountDownLatch createLatch,
                                       final CountDownLatch changeLatch,
                                       final Consul consul, final String path, final String value) {
        try {
            readyLatch.await();
            final ConsulSession session = consul.createSession(getSessionName(), 10, 0);
            assertTrue(consul.writeSessionData(path, value, session.getId()));

            createLatch.await();

            assertTrue(consul.writeSessionData(path, value + "_1", session.getId()));

            changeLatch.await();

            session.close();

        } catch (final InterruptedException ie) {
            fail(ie.getMessage());
        }
    }

    /**
     * Do a simple test with the ConsulWatch class.
     */
    @Test
    public void watches() throws Exception {
        final Consul consul = new Consul(System.getProperty(EP_PROPERTY));
        assertTrue(consul.isValid());
        final ConsulWatch watch = consul.createWatch("some/random/path");
        assertThat(watch, is(notNullValue()));

        final AtomicInteger changeCount = new AtomicInteger(0);

        final String[] values =  new String[] { "first", "second", "third", "fourth", "fifth" };
        final CountDownLatch readyLatch = new CountDownLatch(1);

        final CountDownLatch createLatch = new CountDownLatch(values.length);
        final CountDownLatch changeLatch = new CountDownLatch(values.length);
        final CountDownLatch removeLatch = new CountDownLatch(values.length);
        watch.startWatching(new ConsulWatch.ConsulWatchListener() {
            @Override
            public void created(String valueName, String value) {
                createLatch.countDown();
            }

            @Override
            public void changed(String valueName, String value) {
                changeLatch.countDown();
            }

            @Override
            public void removed(String valueName) {
                removeLatch.countDown();
            }
        });

        final Executor executor = Executors.newCachedThreadPool();
        for (final String str : values) {
            executor.execute(() ->
                    createSessionAndWrite(readyLatch, createLatch, changeLatch, consul, "some/random/path/" + str, str));
        }
        readyLatch.countDown();

        final int waitTime = 1000;
        // N elements should be created
        assertTrue(createLatch.await(waitTime * 10, TimeUnit.MILLISECONDS));

        // ...changed
        assertTrue(changeLatch.await(waitTime * 10, TimeUnit.MILLISECONDS));

        // ...and removed
        assertTrue(removeLatch.await(waitTime * 10, TimeUnit.MILLISECONDS));
    }
}
