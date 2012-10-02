package org.cloudname.example.restapp.rs;

import static javax.ws.rs.core.UriBuilder.fromResource;

import java.net.URI;
import java.util.logging.Logger;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

/**
 * Example resource that can be only accessed by authenticated callers having a particular role.
 *
 * @author jholy
 */
@RolesAllowed({"example-admin"})
@Path("/secure-resource")
public class ExampleSecuredResource {

    private final static Logger log = Logger.getLogger(ExampleSecuredResource.class.getName());

    @Context SecurityContext securityContext;

    /*
     * FIXME 1) Get this working with A3 and a filter; 2) Add an IT test; 3) Ensure HTTPS
     */
    @GET
    public Response getSecuredResource() {
        log.info("getSecuredResource: called by the user " + securityContext.getUserPrincipal().getName());

        URI selfUri = fromResource(ExampleCollectionResource.class).build();
        return Response.ok("There is not much to show, buddy").location(selfUri).build();
    }

}
