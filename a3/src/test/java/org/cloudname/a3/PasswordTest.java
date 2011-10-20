package org.cloudname.a3;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Unit test for Password class.
 *
 * @author borud
 */
public class PasswordTest {
    @Test
    public void testHashing() {
        String mySecret = "setec astronomy";
        assertTrue(Password.matchSecret(mySecret, Password.hashSecret(mySecret)));
        assertFalse(Password.matchSecret("something", Password.hashSecret(mySecret)));
    }

    /**
     * This is not really a unit testing method as such it only exists
     * to provide an informative output about the speed of comparisons.
     */
    @Test
    public void testTiming() {
        int rounds = 100;
        long start = System.currentTimeMillis();
        String pass = "the password";
        String hash = Password.hashSecret(pass);

        System.out.println("Password '" + pass + "' is '" + hash + "'");

        for (int i = 0; i < rounds; i++) {
            Password.matchSecret(pass, hash);
        }

        long time = System.currentTimeMillis() - start;
        System.out.println("*** milliseconds per Password.matchSecret() " + (double)(time/rounds));
    }
}