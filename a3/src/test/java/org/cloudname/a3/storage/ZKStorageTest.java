package org.cloudname.a3.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import org.cloudname.a3.domain.ServiceCoordinate;
import org.cloudname.a3.domain.User;
import org.cloudname.a3.domain.UserDB;
import org.cloudname.testtools.Net;
import org.cloudname.testtools.zookeeper.EmbeddedZooKeeper;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test for ZKStorage.
 *
 * @author borud
 */
public class ZKStorageTest {
    private static Logger log = Logger.getLogger(ZKStorageTest.class.getName());

    private ServiceCoordinate coordinate;
    private UserDB db;
    private EmbeddedZooKeeper ezk;
    Set<String> sampleRoles;
    Map<String,String> sampleProperties;

    @Rule public TemporaryFolder temp = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        coordinate = new ServiceCoordinate.Builder()
            .setDatacenter("dc")
            .setUser("testuser")
            .setServiceName("someservice")
            .build();

        // Set up an embedded ZooKeeper instance
        File rootDir = temp.newFolder("zk-test");
        int port = Net.getFreePort();

        log.info("EmbeddedZooKeeper rootDir=" + rootDir.getCanonicalPath()
                 + ", port=" + port
        );

        // Set up and initialize the embedded ZooKeeper
        ezk = new EmbeddedZooKeeper(rootDir, port);
        ezk.init();

        // Create a simple set of roles
        sampleRoles = new HashSet<String>();
        sampleRoles.add("user");
        sampleRoles.add("foo");
        sampleRoles.add("bar");

        // Create a simple set of properties
        sampleProperties = new HashMap<String, String>();
        sampleProperties.put("foo", "foovalue");
        sampleProperties.put("bar", "barvalue");

        // Create a UserDB instance and populate it with some test users
        final String oldPassword = null;
        final DateTime oldPasswordExpiry = null;
        db = new UserDB();
        db.addUser(new User("alice", "alicehash", "oldpassword", new DateTime(), "Alice Cooper", "alice@example.com", sampleRoles, sampleProperties));
        db.addUser(new User("bob", "bobhash", oldPassword, oldPasswordExpiry, "Bob Cooper", "bob@example.com", sampleRoles, sampleProperties));
        db.addUser(new User("xavier", "xavierhash", oldPassword, oldPasswordExpiry, "Xavier Cooper", "xavier@example.com", Collections.EMPTY_SET, Collections.EMPTY_MAP));
    }

    @Test
    public void testPutGetDropUser() throws Exception {
        User user = new User("walle", "pwordhash", "oldpassword", new DateTime(), "Wall-E", "e@ma.il", Collections.EMPTY_SET, Collections.EMPTY_MAP);
        // FIXME(borud): This user object created above is unused in this test!

        ZKStorage zs = new ZKStorage(coordinate, ezk.getClientConnectionString());
        zs.open();
        zs.createUserDB(db);

        // Fetch the UserDB and make sure that the version is 0 (first version)
        UserDB db2 = zs.getUserDB();
        assertNotNull(db2);
        assertEquals(0, db2.getVersion());

        // Look up a user in the UserDB to make sure there's data there.
        User user2 = db2.getUser("alice");
        assertNotNull(user2);
        assertEquals("alice", user2.getUsername());
        assertEquals("alicehash", user2.getPassword());
        assertEquals("oldpassword", user2.getOldPassword());
        assertNotNull(user2.getOldPasswordExpiry());

        // Just make sure Bob and Xavier are there too.
        assertNotNull(db2.getUser("bob"));
        assertNotNull(db2.getUser("xavier"));

        zs.close();
    }

    /**
     * Make sure that attempt at creating a UserDB that already exists fails.
     */
    @Test (expected = A3StorageException.class)
    public void testDoubleCreation() throws Exception {
        ZKStorage zs = new ZKStorage(coordinate, ezk.getClientConnectionString());
        zs.open();
        zs.createUserDB(db);
        zs.createUserDB(db);
    }

    /**
     * Test the updateUserDB() call.
     */
    @Test
    public void testUpdate() throws Exception {
        ZKStorage zs = new ZKStorage(coordinate, ezk.getClientConnectionString());
        zs.open();
        zs.createUserDB(db);

        UserDB db2 = zs.getUserDB();
        assertNotNull(db2);
        assertEquals(0, db2.getVersion());

        // Add a new user
        db2.addUser(new User("newuser", "newuserhash", "oldpw", new DateTime(), "Newuser Cooper", "newuser@example.com", sampleRoles, sampleProperties));
        zs.updateUserDB(db2);
        assertEquals(1, db2.getVersion());

        // Add another user
        db2.addUser(new User("newuser2", "newuser2hash", "oldpw", new DateTime(), "Newuser2 Cooper", "newuser2@example.com", sampleRoles, sampleProperties));
        zs.updateUserDB(db2);
        assertEquals(2, db2.getVersion());

        // Ensure the users exist
    }


    /**
     * Test that client that has a registered watcher will be notified
     * of changes.
     */
    @Test (timeout = 1000)
    public void testWatcher() throws Exception {
        ZKStorage zs = new ZKStorage(coordinate, ezk.getClientConnectionString());
        zs.open();
        zs.createUserDB(db);

        // Set up a countdown latch that will count down from 4
        final CountDownLatch four = new CountDownLatch(4);

        // Fire up second client 2
        ZKStorage zs2 = new ZKStorage(coordinate, ezk.getClientConnectionString());
        zs2.open();

        // Note to readers: when a watcher is registered an exists()
        // ZooKeeper call is made to ensure we set a watcher.
        zs2.registerUserDBWatcher(
            new UserDBStorage.Watcher() {
                public void onUpdate(UserDB userDB) {
                    System.out.println("Got new database.  Version = " + userDB.getVersion());
                    four.countDown();
                }
            });

        // Now make 4 changes.
        zs.updateUserDB(zs.getUserDB());
        zs.updateUserDB(zs.getUserDB());
        zs.updateUserDB(zs.getUserDB());
        zs.updateUserDB(zs.getUserDB());

        // Await the latch
        four.await();
    }
}