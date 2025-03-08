package org.example.client;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.InputStream;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@Path("/api")
@RegisterRestClient
public interface TestClientService {
  @Path("/test/{testId}")
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  Response createFromInputStream(@PathParam("testId") String testId, InputStream content);

  @Path("/test/{testId}")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  InputStream readFromInputStream(@PathParam("testId") String testId);
}
