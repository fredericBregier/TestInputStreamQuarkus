package org.example.client;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.example.model.TestModel;
import org.jboss.resteasy.reactive.RestPath;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;

@Path("/api")
public interface TestClientService {

  @Path("/test/{testId}")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  Uni<Response> create(@RestPath String testId, TestModel testModel);

  @Path("/test/{testId}")
  @DELETE
  Uni<Response> delete(@RestPath String testId);

  @Path("/test/{testId}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  Uni<TestModel> get(@RestPath String testId);

  @Path("/test/{testId}/binary")
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  Uni<Response> createFromInputStream(@RestPath String testId, InputStream content);

  @Path("/test/{testId}/binary")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  Uni<InputStream> readFromInputStream(@RestPath String testId);

  @Path("/test/{testId}/binarybyte")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  Multi<byte[]> readFromInputStreamByte(@RestPath String testId);

  @Path("/test/{testId}/hugebinary")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  Uni<InputStream> readFromInputStreamHuge(@RestPath String testId);

}
