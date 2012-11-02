package org.cloudname.example.restapp.rs;

import java.util.logging.Logger;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * A simple REST resource
 *
 * @author jholy
 */
@Path("/")
@Produces(MediaType.TEXT_PLAIN)
@Consumes(MediaType.WILDCARD)
public class ExampleCollectionResource {

    private final static Logger log = Logger.getLogger(ExampleCollectionResource.class.getName());

    @GET
    public Response getExampleResource() {
        log.fine("getExampleResource: called");
        // TODO You might want to increase a base counter now (or delete this comment if not)
        return Response.ok("There is not much to show, buddy").build();
    }
}

