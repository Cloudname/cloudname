package org.cloudname.a3.domain;

import java.util.Collections;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;

import org.joda.time.DateTime;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit tests for User
 *
 * @author borud
 */
public class UserTest {

    @Test
    public void testJson() throws Exception {
        Set<String> roles = new HashSet<String>();
        roles.add("admin");
        roles.add("other");

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("foo", "foovalue");
        properties.put("bar", "barvalue");

        final String oldPassword = null;
        final DateTime oldPasswordExpiry = null;

        User user = new User(
            "alice",
            "somehash",
            oldPassword,
            oldPasswordExpiry,
            "Alice Bobson",
            "alice@example.com",
            roles,
            properties);

        String json = user.toJson();

        User user2 = User.fromJson(json);
        assertEquals(user, user2);

        assertTrue(user.hasRole("admin"));
        assertTrue(user.hasRole("other"));
        assertFalse(user.hasRole("somerole"));
    }

    @Test
    public void testEmptyRoles() throws Exception {
        User user = new User("alice",
                             "somehash",
                             "oldpassword",
                             new DateTime(),
                             "Alice Bobson",
                             "alice@example.com",
                             Collections.EMPTY_SET,
                             Collections.EMPTY_MAP);
        User user2 = User.fromJson(user.toJson());

        assertEquals(user, user2);
    }

    @Test (expected = NullPointerException.class)
    public void testNullRoles() throws Exception {
        User user = new User("alice",
                             "somehash",
                             "oldpassword",
                             new DateTime(),
                             "Alice Bobson",
                             "alice@example.com",
                             null,
                             null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testNullOldPasswordExpiry() throws Exception {
        final DateTime noTime = null;
        final Set<String> roles = new HashSet<String>();
        final Map<String, String> properties = new HashMap<String, String>();
        User user = new User("alice",
                             "somehash",
                             "oldpassword",
                             noTime,
                             "Alice Bobson",
                             "alice@example.com",
                             roles,
                             properties);
    }
}