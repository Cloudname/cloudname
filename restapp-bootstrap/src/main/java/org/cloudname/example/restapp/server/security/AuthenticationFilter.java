package org.cloudname.example.restapp.server.security;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import org.cloudname.a3.A3Client;
import org.cloudname.a3.jaxrs.JerseyRequestFilter;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;

/**
 * Check the authentication header and set user principal on the request's security contexts based on it
 * so that Jersey's @RolesAllowed processing can apply it.
 * <p>
 * If there are no security credentials then we let the processing continue
 * (it will fail later if @RolesAllowed or similar is specified on the resource). However if
 * the user is specified but the password is wrong than we let the user know.
 *
 * <h4>Configuration</h4>
 * <p>
 * You need to configure Jersey to use this filter and to support annotations like @RolesAllowed.
 * To enable this filter, set the init parameter
 * <code>com.sun.jersey.spi.container.ContainerRequestFilters</code>
 * to <code>{@link org.cloudname.example.restapp.server.security.AuthenticationFilter}</code>.
 * For instructions what init parameter to set to enable @RolesAllowed etc. see the JavaDoc of
 * the {@link com.sun.jersey.api.container.filter.RolesAllowedResourceFilterFactory}
 *
 * <h4>Implementation</h4>
 * <p>
 * We just delegate to A3's {@link JerseyRequestFilter}. The purpose of this filter
 * is to pass the required parameters to the filter without needing to use the Jersey
 * @{@link javax.ws.rs.ext.Provider} magic.
 *
 * @see com.sun.jersey.api.container.filter.RolesAllowedResourceFilterFactory
 * @see JerseyRequestFilter
 *
 * @author jholy
 */
public class AuthenticationFilter implements ContainerRequestFilter {

    @Context
    UriInfo uriInfo;

    private static A3Client a3Client = null;

    public ContainerRequest filter(ContainerRequest request) {
        // FIXME: Wrong psw => throws MappableContainerException/AuthenticationException
        // instead of st. like new WebApplicationException(Status.UNAUTHORIZED)
        // => use the same setup as Sylfide to map the exception properly to 401
        ContainerRequest requestAuthenticated = new JerseyRequestFilter(uriInfo, a3Client).filter(request);
        return requestAuthenticated;
    }

    public static void setA3Client(A3Client a3Client) {
        AuthenticationFilter.a3Client = a3Client;
    }
}
