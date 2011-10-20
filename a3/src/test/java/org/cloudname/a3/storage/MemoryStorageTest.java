package org.cloudname.a3.storage;

import org.cloudname.a3.domain.UserDB;

import java.io.InputStream;

import org.junit.*;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;

/**
 * Unit tests for MemoryStorage.
 *
 * @author borud
 */
public class MemoryStorageTest {
    private static final String USERDB_FOR_TEST = "/userdb-for-test.json";

    @Test
    public void testSimple() {
        MemoryStorage m = new MemoryStorage();
    }

    @Test
    public void testReadingFromInputStream() throws Exception {
        // Get the embedded testing user database from resources
        InputStream is = getClass().getResourceAsStream(USERDB_FOR_TEST);
        assertNotNull(is);

        // Create a memory storage and populate it with data from the
        // InputStream
        MemoryStorage m = MemoryStorage.fromInputStream(is);
        assertNotNull(m);

        // Get the user database and make sure the users we want are there
        UserDB db = m.getUserDB();
        assertNotNull(db);
        assertEquals("Oyvind Bakksjo", db.getUser("bakksjo").getRealName());
        assertEquals("Bjorn Borud", db.getUser("borud").getRealName());
        m.close();
    }

    /**
     * Test creation.
     */
    @Test
    public void testCreate() {
        MemoryStorage m = new MemoryStorage();
        m.open();
        m.createUserDB(new UserDB());

        UserDB db = m.getUserDB();

        m.updateUserDB(db);
        m.close();
    }

    /**
     * Try to update a UserDB with the wrong version.
     */
    @Test (expected = A3StorageException.class)
    public void testCreateWithWrongVersion() {
        MemoryStorage m = new MemoryStorage();
        m.open();
        m.createUserDB(new UserDB());

        // Since we do not do any defensive copies this is a bit silly
        // but it does verify the behavior.  The version will be 0 and
        // it has to be 0 when we do an update since it is an update
        // from version 0
        UserDB db = m.getUserDB();
        m.updateUserDB(db);

        // After the update the version should be 1
        assertEquals(1, m.getUserDB().getVersion());

        // Now insert a new UserDB with version = 0, which will
        // produce an error since MemoryStorage expects version = 1
        UserDB db2 = new UserDB();
        db2.setVersion(0);
        m.updateUserDB(db2);

        // No point in closing since we expect exception on the above
        // line.
    }
}

