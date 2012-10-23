package org.cloudname.a3.jaxrs;

import com.sun.jersey.api.container.MappableContainerException;
import com.sun.jersey.api.model.AbstractMethod;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerResponseFilter;
import com.sun.jersey.spi.container.ResourceFilter;
import com.sun.jersey.spi.container.ResourceFilterFactory;

import java.util.Collections;
import java.util.List;

import javax.annotation.security.DenyAll;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;

import javax.ws.rs.WebApplicationException;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

/**
 * This class is based on
 * {@link com.sun.jersey.api.container.filter.RolesAllowedResourceFilterFactory}
 * and also supports <code>@RolesAllowed</code> etc.
 * but with the modification that unauthenticated, unauthorized
 * requests will result in a HTTP 401 Unauthorized, rather than a 403
 * Forbidden, thus indicating (on unsuccessful requests) to
 * non-authenticating clients that they should try sending
 * authenticated requests.
 * <p>
 * To enable this filter, set the Jersey init parameter
 * <code>com.sun.jersey.spi.container.ContainerRequestFilters</code>
 * to <code>{@link org.cloudname.a3.jaxrs.JerseyRoleBasedAccessControlResourceFilterFactory}</code>.
 * (See the JavaDoc of {@link com.sun.jersey.api.container.filter.RolesAllowedResourceFilterFactory}
 * for details.)
 * <p>
 * It would be preferable to _wrap_ an instance of
 * RolesAllowedResourceFilterFactory and use decoration to achieve our
 * slight twist, rather than replicate its entire source (and miss out
 * on updates), but since that class relies on container injection of
 * a private field, we are unable to instantiate it properly. How
 * lovely life is when people write code only for containers.
 * <p>
 * Apologies for the name. This is a factory that creates resource
 * filters that provide role-based access control when used with
 * Jersey. I couldn't come up with a shorter name that wasn't
 * ambiguous...
 */
public class JerseyRoleBasedAccessControlResourceFilterFactory
    implements ResourceFilterFactory
{
    private static final String REALM = "schmealm";
    private @Context SecurityContext sc;

    private class Filter implements ResourceFilter, ContainerRequestFilter {

        private final boolean denyAll;
        private final String[] rolesAllowed;

        protected Filter() {
            this.denyAll = true;
            this.rolesAllowed = null;
        }

        protected Filter(String[] rolesAllowed) {
            this.denyAll = false;
            this.rolesAllowed = (rolesAllowed != null) ? rolesAllowed : new String[] {};
        }

        // ResourceFilter

        @Override
        public ContainerRequestFilter getRequestFilter() {
            return this;
        }

        @Override
        public ContainerResponseFilter getResponseFilter() {
            return null;
        }

        // ContainerRequestFilter

        @Override
        public ContainerRequest filter(ContainerRequest request) {
            if (!denyAll) {
                for (String role : rolesAllowed) {
                    if (sc.isUserInRole(role))
                        return request;
                }
            }

            // This is our modification to the original filter:
            // ---------------------------------------------------
            if (sc.getAuthenticationScheme() == null) {
                // Authentication might help!
                throw new MappableContainerException(
                        new AuthenticationException(
                            "Needs authentication credentials\r\n",
                            REALM));
            }

            // Request is authenticated, nothing to do but say no.
            // ---------------------------------------------------

            throw new WebApplicationException(Response.Status.FORBIDDEN);
        }
    }

    @Override
    public List<ResourceFilter> create(AbstractMethod am) {
        // DenyAll on the method take precedence over RolesAllowed and PermitAll
        if (am.isAnnotationPresent(DenyAll.class))
            return Collections.<ResourceFilter>singletonList(new Filter());

        // RolesAllowed on the method takes precedence over PermitAll
        RolesAllowed ra = am.getAnnotation(RolesAllowed.class);
        if (ra != null)
            return Collections.<ResourceFilter>singletonList(new Filter(ra.value()));

        // PermitAll takes precedence over RolesAllowed on the class
        if (am.isAnnotationPresent(PermitAll.class))
            return null;

        // RolesAllowed on the class takes precedence over PermitAll
        ra = am.getResource().getAnnotation(RolesAllowed.class);
        if (ra != null)
            return Collections.<ResourceFilter>singletonList(new Filter(ra.value()));

        // No need to check whether PermitAll is present.
        return null;
    }
}
