package org.cloudname.example.restapp.server;

import java.security.Principal;
import java.util.logging.Logger;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import com.sun.jersey.core.util.Base64;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;

/**
 * Check the authentication header and set user principal on the request's security contexts based on it.
 * <p>
 * If there are no security credentials then we let the processing continue
 * (it will fail later if @RolesAllowed or similar is specified on the resource). However if
 * the user is specified but the password is wrong than we let the user know.
 * <p>
 * You need to configure Jersey to use this filter and to support annotations like @RolesAllowed.
 * To enable this filter, set the init parameter
 * <code>com.sun.jersey.spi.container.ContainerRequestFilters</code>
 * to <code>{@link org.cloudname.example.restapp.server.AuthenticationFilter}</code>.
 * For instructions what init parameter to set to enable @RolesAllowed etc. see the JavaDoc of
 * the {@link com.sun.jersey.api.container.filter.RolesAllowedResourceFilterFactory}
 *
 * @see http
 *      ://stackoverflow.com/questions/9591227/custom-rolesallowed-roles-in-jersey-webservice-with-containerrequestfilter
 * @see com.sun.jersey.api.container.filter.RolesAllowedResourceFilterFactory
 *
 * @author jholy
 */
public class AuthenticationFilter implements ContainerRequestFilter {

    private final static Logger log = Logger.getLogger(AuthenticationFilter.class.getName());

    /**
     * User object returned when there are no authentication credentials.
     * <p>
     * It seems that the RolesAllowedResourceFilterFactory requires a non-null
     * Authorizer to respect @RolesAllowed (i.e. if the user authorizer is null then it would
     * not prevent the call of a secured method).
     */
    private final static User ANONYMOUS_USER = new User("anonymous", null);

    @Context
    UriInfo uriInfo;

    public ContainerRequest filter(ContainerRequest request) {
        User user = authenticate(request);
        request.setSecurityContext(new Authorizer(user));
        return request;
    }

    /**
     * Try to authenticate the user, if authentication credentials provided in the request
     * @param request (required)
     * @return {@link #ANONYMOUS_USER} if no authentication credentials
     * @throws WebApplicationException BAD_REQUEST if wrong authentication method or invalid authentication
     * header format, UNAUTHORIZED if invalid username/password combination
     */
    private User authenticate(ContainerRequest request) {
        // Extract authentication credentials
        String authentication = request.getHeaderValue(ContainerRequest.AUTHORIZATION);

        if (authentication == null) {
            log.fine("No authentication credentials provided for " + uriInfo.getRequestUri());
            return ANONYMOUS_USER;
        }

        if (!authentication.startsWith("Basic ")) {
            log.info("Invalid authentication method - only HTTP Basic authentication is supported, got "
                    + authentication);
            throw new WebApplicationException(Status.BAD_REQUEST);
        }

        authentication = authentication.substring("Basic ".length());
        String[] values = Base64.base64Decode(authentication).split(":");
        if (values.length < 2) {
            log.info("Wrong format of the authentication string (missing ':'?)");
            throw new WebApplicationException(Status.BAD_REQUEST);
        }
        String username = values[0];
        String password = values[1];

        if ((username == null) || (password == null)) {
            log.info("Password or username not provided");
            throw new WebApplicationException(Status.BAD_REQUEST);
        }

        final User user = authenticate(username, password);

        if (user == null) {
            // Username + password provided but do not match our user database
            throw new WebApplicationException(Status.UNAUTHORIZED);
        }

        return user;
    }

    private User authenticate(String username, String password) {
        // FIXME authenticate using A3
        if (username.equals("testUser") && password.equals("testUser_psw")) {
            return new User(username, "example-admin");
        } else if (username.equals("anotherTestUser") && password.equals("anotherTestUser_psw")) {
            return new User(username, "guest-only");
        } else {
            log.info("Unknown user " + username);
            return null;
        }
    }

    public class Authorizer implements SecurityContext {

        private User user;
        private Principal principal;

        public Authorizer(final User user) {
            this.user = user;
            this.principal = new Principal() {
                public String getName() {
                    return user.username;
                }
            };
        }

        public Principal getUserPrincipal() {
            return this.principal;
        }

        public boolean isUserInRole(String role) {
            return (role.equals(user.role));
        }

        public boolean isSecure() {
            return "https".equals(uriInfo.getRequestUri().getScheme());
        }

        public String getAuthenticationScheme() {
            return SecurityContext.BASIC_AUTH;
        }
    }

    public static class User {

        public String username;
        public String role;

        public User(String username, String role) {
            this.username = username;
            this.role = role;
        }
    }

}
