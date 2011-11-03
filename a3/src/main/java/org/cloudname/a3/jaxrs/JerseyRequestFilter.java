package org.cloudname.a3.jaxrs;

import com.sun.jersey.api.container.MappableContainerException;

import com.sun.jersey.core.util.Base64;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;

import java.security.Principal;

import javax.ws.rs.WebApplicationException;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import org.cloudname.a3.A3Client;
import org.cloudname.a3.A3Principal;
import org.cloudname.a3.AuthnResult;

import org.cloudname.a3.domain.User;

public class JerseyRequestFilter implements ContainerRequestFilter {

    private static final String REALM = "Schmealm";

    private final UriInfo uriInfo;
    private final A3Client a3Client;

    public JerseyRequestFilter(
        @Context final UriInfo uriInfo,
        @Context final A3Client a3Client)
    {
        this.uriInfo = uriInfo;
        this.a3Client = a3Client;
    }

    @Override
    public ContainerRequest filter(final ContainerRequest request) {
        final User user = authenticate(request);
        if (user != null) {
            request.setSecurityContext(new AuthenticatedAuthorizer(user));
        } else {
            request.setSecurityContext(new UnauthenticatedAuthorizer());
        }
        return request;
    }

    private User authenticate(final ContainerRequest request) {
        // Extract authentication header.
        final String authenticationHeader = request.getHeaderValue(
            ContainerRequest.AUTHORIZATION);
        if (authenticationHeader == null) {
            // No authentication, no user.
            return null;
            // Enable this instead if all clients are supposed to always authenticate.
            // throw new MappableContainerException(
            //     new AuthenticationException(
            //         "Authentication required\r\n", REALM));
        }
        if (!authenticationHeader.startsWith("Basic ")) {
            throw new MappableContainerException(
                new AuthenticationException(
                    "Only supported authentication scheme is HTTP Basic\r\n",
                    REALM));
        }

        // Extract credentials.
        final String username;
        final String password;
        {
            final String authenticationEncoded = authenticationHeader
                .substring("Basic ".length());
            final String authenticationDecoded = Base64
                .base64Decode(authenticationEncoded);
            final String[] values = authenticationDecoded.split(":");
            if (values.length < 2) {
                throw new MappableContainerException(
                    new AuthenticationException(
                        "Invalid syntax for username or password\r\n", REALM));
            }
            username = values[0];
            password = values[1];
            // These won't be null (String.split() doesn't work that way).
        }

        if (username.isEmpty() || password.isEmpty()) {
            throw new MappableContainerException(
                new AuthenticationException(
                    "Invalid username or password\r\n", REALM));
        }

        // Authenticate.
        final AuthnResult authnResult
            = a3Client.authenticate(username, password);
        if (!authnResult.isOk()) {
            throw new MappableContainerException(
                new AuthenticationException(
                    "Invalid username or password\r\n", REALM));
        }

        return authnResult.getUser();
    }

    // Non-static because it needs access to the uriInfo field.
    private class AuthenticatedAuthorizer implements SecurityContext {
        private final User user;
        private final Principal principal;

        public AuthenticatedAuthorizer(final User user) {
            this.user = user;
            this.principal = new A3Principal(user);
        }

        @Override
        public Principal getUserPrincipal() {
            return principal;
        }

        @Override
        public boolean isUserInRole(final String role) {
            return user.hasRole(role);
        }

        @Override
        public boolean isSecure() {
            return "https".equals(uriInfo.getRequestUri().getScheme());
        }

        @Override
        public String getAuthenticationScheme() {
            return SecurityContext.BASIC_AUTH;
        }
    }

    // Non-static because it needs access to the uriInfo field.
    private class UnauthenticatedAuthorizer implements SecurityContext {
        @Override
        public Principal getUserPrincipal() {
            return null;
        }

        @Override
        public boolean isUserInRole(final String role) {
            // Unauthenticated requests are in no roles.
            return false;
        }

        @Override
        public boolean isSecure() {
            return "https".equals(uriInfo.getRequestUri().getScheme());
        }

        @Override
        public String getAuthenticationScheme() {
            // Request was not authenticated.
            return null;
        }
    }
}
