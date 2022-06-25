package org.example.server;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import org.example.fake.FakeInputStream;
import org.example.model.TestModel;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestPath;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;

import static org.example.StaticValues.*;

@ApplicationScoped
@Path("/api")
public class QuarkusResource {
  private static final Logger LOG = Logger.getLogger(QuarkusResource.class);


  @Path("/test/{testId}")
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Uni<Response> create(@RestPath String testId, TestModel testModel) {
    return Uni.createFrom().emitter(em -> {
      em.complete(Response.status(200).build());
    });
  }

  @Path("/test/{testId}")
  @DELETE
  public Uni<Response> delete(@RestPath String testId) {
    return Uni.createFrom().emitter(em -> {
      em.complete(Response.status(200).build());
    });
  }

  @Path("/test/{testId}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<TestModel> get(@RestPath String testId) {
    return Uni.createFrom().emitter(em -> {
      final TestModel testModel = new TestModel("val1", "val2");
      em.complete(testModel);
    });
  }


  @Path("/test/{testId}/binary")
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<Response> createFromInputStream(@RestPath String testId, InputStream content) {
    return Uni.createFrom().emitter(em -> {
      try {
        FakeInputStream.consumeAll(content);
        em.complete(Response.status(200).build());
      } catch (IOException e) {
        LOG.error("Quarkus: " + e.getMessage(), e);
        em.complete(Response.status(500).build());
      }
    });
  }

  @Path("/test/{testId}/binary")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<InputStream> readFromInputStream(@RestPath String testId) {
    Uni<InputStream> uni = Uni.createFrom().emitter(em -> {
      em.complete(new FakeInputStream(INPUTSTREAM_SMALL_SIZE));
    });
    uni = uni.onFailure().recoverWithNull();
    return uni;
  }

  @Path("/test/{testId}/binarybyte")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<InputStream> readFromInputStreamByte(@RestPath String testId) {
    Uni<InputStream> uni = Uni.createFrom().emitter(em -> {
      em.complete(new FakeInputStream(INPUTSTREAM_SMALL_SIZE));
    });
    uni = uni.onFailure().recoverWithNull();
    return uni;
  }

  @Path("/test/{testId}/hugebinary")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<InputStream> readFromInputStreamHuge(@RestPath String testId) {
    Uni<InputStream> uni = Uni.createFrom().emitter(em -> {
      em.complete(new FakeInputStream(INPUTSTREAM_SIZE));
    });
    uni = uni.onFailure().recoverWithNull();
    return uni;
  }

}