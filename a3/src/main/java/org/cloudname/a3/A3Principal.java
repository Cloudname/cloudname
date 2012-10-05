package org.cloudname.a3;

import java.security.Principal;

import org.cloudname.a3.domain.User;

public class A3Principal implements Principal {
    private final User authenticatedUser;

    public A3Principal(final User authenticatedUser) {
        this.authenticatedUser = authenticatedUser;
    }

    @Override
    public String getName() {
        return authenticatedUser.getUsername();
    }

    public User getAuthenticatedUser() {
        return authenticatedUser;
    }

    @Override
    public String toString() {
        return "A3Principal[" + getName() + "]";
    }
}
