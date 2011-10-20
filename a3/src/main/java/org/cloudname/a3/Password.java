package org.cloudname.a3;

import org.mindrot.jbcrypt.BCrypt;

/**
 * Password hashing and verification utility. This is not so much a
 * utility as a central location for documenting the password hashing
 * parameters we use.
 *
 * from http://www.mindrot.org/files/jBCrypt/README :
 *
 *   jBCrypt is an implementation the OpenBSD Blowfish password hashing
 *   algorithm, as described in "A Future-Adaptable Password Scheme" by
 *   Niels Provos and David Mazieres:
 *   http://www.openbsd.org/papers/bcrypt-paper.ps

 *   This system hashes passwords using a version of Bruce Schneier's
 *   Blowfish block cipher with modifications designed to raise the cost
 *   of off-line password cracking. The computation cost of the
 *   algorithm is parameterised, so it can be increased as computers get
 *   faster.
 *
 * @author borud
 */
public class Password {
    // matchSecret takes 13ms for the value 7 on a MacBook Pro 2.66Ghz.
    // matchSecret takes 3ms for the value 5 on a MacBook Pro 2.66Ghz.
    // matchSecret takes 1ms for the value 4 on a MacBook Pro 2.66Ghz.
    //
    public static final int BCRYPT_LOG_ROUNDS = 4;

    public static String hashSecret(String secret) {
        return BCrypt.hashpw(secret, BCrypt.gensalt(BCRYPT_LOG_ROUNDS));
    }

    public static boolean matchSecret(String secret, String hash) {
        return BCrypt.checkpw(secret, hash);
    }
}