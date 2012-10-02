package org.cloudname.example.restapp.rs;

import static javax.ws.rs.core.UriBuilder.fromResource;

import java.net.URI;
import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * A simple REST resource
 *
 * @author jholy
 */
@Path("/")
public class ExampleCollectionResource {

    private final static Logger log = Logger.getLogger(ExampleCollectionResource.class.getName());

    /**
     * Info about the request, injected by Jersey at each call.
     */
    @Context UriInfo uriInfo;

    @GET
    public Response getExampleResource() {
        log.info("getExampleResource: called");
        URI selfUri = fromResource(ExampleCollectionResource.class).build();
        return Response.ok("There is not much to show, buddy").location(selfUri).build();
    }
}

