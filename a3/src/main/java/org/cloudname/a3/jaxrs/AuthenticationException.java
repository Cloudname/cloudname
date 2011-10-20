package org.cloudname.a3.jaxrs;

/**
 * <p>A runtime exception representing a failure to provide correct
 * authentication credentials.</p>
 */
public class AuthenticationException extends RuntimeException {
    private final String realm;

    public AuthenticationException(final String message, final String realm) {
        super(message);
        this.realm = realm;
    }

    public String getRealm() {
        return realm;
    }
}
