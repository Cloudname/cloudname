package org.cloudname.a3;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.cloudname.a3.domain.User;
import org.cloudname.a3.domain.UserDB;
import org.cloudname.a3.storage.MemoryStorage;
import org.cloudname.a3.storage.UserDBStorage;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class A3ClientTest {

    private UserDBStorage createStorage() {
        // Populate a set of sample roles.
        final Set<String> sampleRoles = new HashSet<>();
        sampleRoles.add("admin");
        sampleRoles.add("other");


        // Create a simple set of properties
        final Map<String,String> sampleProperties = new HashMap<>();
        sampleProperties.put("foo", "foovalue");
        sampleProperties.put("bar", "barvalue");
        sampleProperties.put("foobar", "foovalue barvalue");

        // Create a UserDB instance and populate it with some test users
        final UserDB db = new UserDB();
        db.addUser(new User("alice", "$2a$04$ScORS0wG.JjulVc3fJlcTuGJfG8GSuIRyRun0Gu7KmgwDuY0VSdOK",
                null, null, "Alice Cooper", "alice@example.com", sampleRoles, sampleProperties));
        db.addUser(new User("bob", "$2a$04$ScORS0wG.JjulVc3fJlcTuGJfG8GSuIRyRun0Gu7KmgwDuY0VSdOK",
                "$2a$04$CjpjqV138U0ErnCS6PCPzuhxjYTFbfw0hCmR7jI3Lgb6EjMusvfrS",
                DateTime.parse("1011-10-14"), "Bob Cooper", "bob@example.com", sampleRoles,
                sampleProperties));
        db.addUser(new User("xavier",
                "$2a$04$ScORS0wG.JjulVc3fJlcTuGJfG8GSuIRyRun0Gu7KmgwDuY0VSdOK",
                "$2a$04$DiaNRN4WSQakBCDhzTYgAOozTFQdWc6hBO96th4J5MDzCBueTWq1u",
                DateTime.parse("3011-10-14"), "Xavier Cooper", "xavier@example.com", sampleRoles,
                sampleProperties));

        final UserDBStorage dbStorage = new MemoryStorage();
        dbStorage.open();
        dbStorage.createUserDB(db);
        return dbStorage;
    }


    @Test (timeout=500)
    public void testEverything() throws Exception {
        A3Client a3 = new A3Client(createStorage());
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

        {
            GetUserResult getUserResult = a3.getUserByProperty("foobar", "foovalue");
            assertTrue(getUserResult.isOk());
        }

        {
            GetUserResult getUserResult = a3.getUserByProperty("foobar", "barvalue");
            assertTrue(getUserResult.isOk());
        }

        {
            GetUserResult getUserResult = a3.getUserByProperty("unexist", "unexistvalue");
            assertFalse(getUserResult.isOk());
        }
    }
}
