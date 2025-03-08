package org.example.server;

import static org.example.StaticValues.*;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.ServerErrorException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import org.example.fake.FakeInputStream;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestPath;

@Path("/api")
public class QuarkusResource {
  private static final Logger LOG = Logger.getLogger(QuarkusResource.class);

  @Path("/test/{testId}")
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<Response> createFromInputStream(
      @RestPath final String testId, final InputStream content) {
    return Uni.createFrom()
        .emitter(
            em -> {
              try {
                FakeInputStream.consumeAll(content);
                em.complete(Response.status(200).build());
                LOG.info("Quarkus create done");
              } catch (final IOException e) {
                LOG.error("Quarkus: " + e.getMessage(), e);
                em.fail(new ServerErrorException(Response.Status.BAD_REQUEST, e));
              }
            });
  }

  @Path("/test/{testId}")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<InputStream> readFromInputStream(@RestPath final String testId) {
    return Uni.createFrom()
        .emitter(
            em -> {
              em.complete(new FakeInputStream(INPUTSTREAM_SIZE));
              LOG.info("Quarkus read done");
            });
  }
}
