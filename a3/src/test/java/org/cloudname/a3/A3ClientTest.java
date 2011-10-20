package org.cloudname.a3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import org.cloudname.a3.domain.ServiceCoordinate;
import org.cloudname.a3.domain.User;
import org.cloudname.a3.domain.UserDB;
import org.cloudname.a3.storage.UserDBStorage;
import org.cloudname.a3.storage.ZKStorage;
import org.cloudname.testtools.Net;
import org.cloudname.testtools.zookeeper.EmbeddedZooKeeper;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class A3ClientTest {
    private static Logger log = Logger.getLogger(A3ClientTest.class.getName());
    private ServiceCoordinate coordinate;
    private UserDB db;
    private EmbeddedZooKeeper ezk;
    private ZKStorage zkUserDBStorage;
    private Set<String> sampleRoles;
    Map<String,String> sampleProperties;

    @Rule public TemporaryFolder temp = new TemporaryFolder();

    /**
     * Set up an embedded ZooKeeper instance for testing.  Also create
     * a UserDB and populate it into the ZooKeeper instance.
     */
    @Before
    public void setUp() throws Exception {
        coordinate = new ServiceCoordinate.Builder()
            .setDatacenter("dc")
            .setUser("testuser")
            .setServiceName("test-service")
            .build();

        // Set up an embedded ZooKeeper instance
        File rootDir = temp.newFolder("zk-test");
        int port = Net.getFreePort();

        log.info("EmbeddedZooKeeper rootDir=" + rootDir.getCanonicalPath()
                 + ", port=" + port
        );

        // Populate a set of sample roles.
        sampleRoles = new HashSet<String>();
        sampleRoles.add("admin");
        sampleRoles.add("other");


        // Create a simple set of properties
        sampleProperties = new HashMap<String, String>();
        sampleProperties.put("foo", "foovalue");
        sampleProperties.put("bar", "barvalue");

        // Set up and initialize the embedded ZooKeeper
        ezk = new EmbeddedZooKeeper(rootDir, port);
        ezk.init();

        // Create a UserDB instance and populate it with some test users
        db = new UserDB();
        db.addUser(new User("alice", "$2a$04$ScORS0wG.JjulVc3fJlcTuGJfG8GSuIRyRun0Gu7KmgwDuY0VSdOK", null, null, "Alice Cooper", "alice@example.com", sampleRoles, sampleProperties));
        db.addUser(new User("bob", "$2a$04$ScORS0wG.JjulVc3fJlcTuGJfG8GSuIRyRun0Gu7KmgwDuY0VSdOK", "$2a$04$CjpjqV138U0ErnCS6PCPzuhxjYTFbfw0hCmR7jI3Lgb6EjMusvfrS", DateTime.parse("1011-10-14"), "Bob Cooper", "bob@example.com", sampleRoles, sampleProperties));
        db.addUser(new User("xavier", "$2a$04$ScORS0wG.JjulVc3fJlcTuGJfG8GSuIRyRun0Gu7KmgwDuY0VSdOK", "$2a$04$DiaNRN4WSQakBCDhzTYgAOozTFQdWc6hBO96th4J5MDzCBueTWq1u", DateTime.parse("3011-10-14"), "Xavier Cooper", "xavier@example.com", sampleRoles, sampleProperties));

        // Create a user database in ZooKeeper
        zkUserDBStorage = new ZKStorage(coordinate, ezk.getClientConnectionString());
        zkUserDBStorage.open();
        zkUserDBStorage.createUserDB(db);
    }


    @Test (timeout=500)
    public void testEverything() throws Exception {
        A3Client a3 = new A3Client(zkUserDBStorage);
        a3.open();

        // Try authentication with the correct password
        {
            AuthnResult result = a3.authenticate("alice", "the password");
            assertNotNull(result);
            assertTrue(result.isOk());
            assertEquals(AuthnResult.State.OK, result.getState());
        }

        // Try authentication with the wrong password
        {
            AuthnResult result = a3.authenticate("alice", "the wrong password");
            assertNotNull(result);
            assertTrue(result.isWrongPassword());
            assertEquals(AuthnResult.State.WRONG_PASSWORD, result.getState());
        }

        // Try authentication with old, expired password
        {
            AuthnResult result = a3.authenticate("bob", "hjemmelaga");
            assertNotNull(result);
            assertTrue(result.isWrongPassword());
            assertEquals(AuthnResult.State.WRONG_PASSWORD, result.getState());
        }

        // Try authentication with old, but unexpired password
        {
            AuthnResult result = a3.authenticate("xavier", "fredagskos");
            assertNotNull(result);
            assertTrue(result.isOk());
            assertEquals(AuthnResult.State.OK, result.getState());
        }

        // Try authentication with bogus user
        {
            AuthnResult result = a3.authenticate("unexist", "the password");
            assertNotNull(result);
            assertEquals(AuthnResult.State.UNKNOWN_USER, result.getState());
        }

        // Now we change the database a bit, update it and try again
        UserDB newDB = zkUserDBStorage.getUserDB();
        newDB.addUser(new User("unexist", "$2a$04$ScORS0wG.JjulVc3fJlcTuGJfG8GSuIRyRun0Gu7KmgwDuY0VSdOK", null, null, "Xavier Cooper", "xavier@example.com", sampleRoles, sampleProperties));

        // We use the countdown latch to give the system approximately
        // enough time to propagate the change.  Since the changes
        // should be propagated almost simultaneously we use this to
        // reduce the amount of busy-waiting done in the while()-loop.

        final CountDownLatch one = new CountDownLatch(1);
        zkUserDBStorage.registerUserDBWatcher(new UserDBStorage.Watcher() {
                public void onUpdate(UserDB udb) {
                    one.countDown();
                }
            });

        zkUserDBStorage.updateUserDB(newDB);
        one.await();

        // If this loop does not terminate before the timeout of this
        // unit test expires then the test failed.  The loop will
        // terminate when a3 sees the user database update and the
        // "unexist" user comes into existence with the correct
        // password.
        while (! a3.authenticate("unexist", "the password").isOk()) {
            // Just sleep 5ms
            Thread.sleep(5);
        }
    }
}