package org.cloudname.a3.jaxrs;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * <p>Map an authentication exception to an HTTP 401 response, optionally
 * including the realm for a credentials challenge at the client.</p>
 */
@Provider
public class AuthenticationExceptionMapper
    implements ExceptionMapper<AuthenticationException>
{
    public Response toResponse(AuthenticationException e) {
        final Response.ResponseBuilder responseBuilder = Response
            .status(Status.UNAUTHORIZED)
            .type("text/plain")
            .entity(e.getMessage());
        if (e.getRealm() != null) {
            responseBuilder.header(
                "WWW-Authenticate",
                "Basic realm=\"" + e.getRealm() + "\"");
        }
        return responseBuilder.build();
    }
}
