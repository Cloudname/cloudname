package org.cloudname.a3.domain;

import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;

import org.joda.time.DateTime;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for User
 *
 * @author borud
 */
public class UserDBTest {
    @Test
    public void simpleTest() {
        Set<String> roles = new HashSet<String>();
        roles.add("admin");
        roles.add("other");

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("foo", "foovalue");
        properties.put("bar", "barvalue");

        UserDB db = new UserDB();
        final String nullPassword = null;
        final DateTime nullExpiry = null;
        db.addUser(new User("alice", "alicehash", nullPassword, nullExpiry, "Alice Cooper", "alice@example.com", roles, properties));
        db.addUser(new User("bob", "bobhash", nullPassword, nullExpiry, "Bob Cooper", "bob@example.com", roles, properties));

        // Preliminary test
        assertNotNull(db.getUser("alice"));
        assertEquals("Alice Cooper", db.getUser("alice").getRealName());

        // Convert to JSON format and then create a UserDB from that
        // JSON format to ensure they are equal.
        String json = db.toJson();
        UserDB db2  = UserDB.fromJson(json);
        String json2 = db2.toJson();

        assertEquals(json, json2);
        assertNotNull(db2.getUser("alice"));
        assertEquals(db.getUser("alice"), db2.getUser("alice"));
        assertEquals(db.getUser("bob"), db2.getUser("bob"));
    }
}
