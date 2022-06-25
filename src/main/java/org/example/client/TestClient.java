package org.example.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.stream.ChunkedStream;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Promise;
import io.quarkus.arc.Arc;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.mutiny.core.Vertx;
import org.eclipse.microprofile.rest.client.RestClientBuilder;
import org.example.client.netty.HttpClientInitializer;
import org.example.client.netty.RequestType;
import org.example.model.TestModel;
import org.example.quarkus.AsyncInputStream;
import org.example.quarkus.BytesToInputStream;
import org.example.quarkus.NettyToInputStream;
import org.example.quarkus.VertxToInputStream;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestPath;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.CDI;
import javax.inject.Inject;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.netty.buffer.Unpooled.*;
import static org.example.StaticValues.*;
import static org.example.client.netty.HttpClientHandler.*;

@ApplicationScoped
public class TestClient {
  private static final Logger LOG = Logger.getLogger(TestClient.class);
  private static final EventLoopGroup group = new NioEventLoopGroup();
  private static final Bootstrap clientBootsrap;
  private static final String API_TEST = "/api/test/";
  private static final String BINARY = "/binary";
  private static final String HUGE_BINARY = "/hugebinary";
  private static final Duration DURATION_30S = Duration.ofSeconds(30);
  private static final String RESPONSE_NOT_GET = "No Response";

  static {
    clientBootsrap = new Bootstrap();
    clientBootsrap.group(group).channel(NioSocketChannel.class).handler(new HttpClientInitializer());
  }

  @Inject
  public Vertx vertx;
  TestClientService testClientService;
  private String hostname = "localhost";
  private int quarkusPort = 8081;
  private Channel clientChannel;
  private WebClient webClient;
  private final ObjectMapper objectMapper = Arc.container().instance(ObjectMapper.class).get();

  public TestClient() {
    if (vertx == null) {
      LOG.warn("Vertx Should be injected but isn't: Use programmatic injection");
      vertx = CDI.current().select(Vertx.class).get();
    }
  }

  public static void closeGlobal() {
    group.shutdownGracefully();
  }

  public TestClient prepare(String hostname, int quarkusPort) {
    this.hostname = hostname;
    this.quarkusPort = quarkusPort;
    testClientService =
        RestClientBuilder.newBuilder().baseUri(URI.create("http://" + hostname + ':' + quarkusPort + '/'))
                         .build(TestClientService.class);
    return this;
  }

  public boolean create(String testId, TestModel testModel) {
    final Uni<Response> uni = testClientService.create(testId, testModel);
    try (final Response response = uni.await().atMost(DURATION_30S)) {
      if (response == null) {
        // Issue
        LOG.error(RESPONSE_NOT_GET);
        return false;
      }
      if (response.getStatus() >= 400) {
        // Issue
        LOG.error("Status: " + response.getStatus());
        return false;
      }
      return true;
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean delete(@RestPath final String testId) {
    final Uni<Response> uni = testClientService.delete(testId);
    try (final Response response = uni.await().atMost(DURATION_30S)) {
      if (response == null) {
        // Issue
        LOG.error(RESPONSE_NOT_GET);
        return false;
      }
      if (response.getStatus() >= 400) {
        // Issue
        LOG.error("Status: " + response.getStatus());
        return false;
      }
      return true;
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public TestModel get(@RestPath final String testId) {
    final Uni<TestModel> uni = testClientService.get(testId);
    try {
      final TestModel testModel = uni.await().atMost(DURATION_30S);
      if (testModel == null) {
        // Issue
        LOG.error(RESPONSE_NOT_GET);
        return null;
      }
      return testModel;
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  public void closeNetty() {
    if (clientChannel != null && !clientChannel.closeFuture().isDone()) {
      clientChannel.close();
      clientChannel = null;
    }
  }

  private void createChannel(String hostname, int port) throws InterruptedException {
    if (clientChannel == null || clientChannel.closeFuture().isDone()) {
      clientChannel = clientBootsrap.connect(hostname, port).sync().channel();
    }
  }

  public boolean createNetty(String testId, TestModel testModel) {
    try {
      final Promise<Boolean> future = new DefaultPromise<>(group.next());
      createChannel(hostname, quarkusPort);
      clientChannel.attr(requestTypeAttributeKey).set(RequestType.requestPost(future));
      final String json = objectMapper.writeValueAsString(testModel);
      ByteBuf byteBuf = ByteBufUtil.writeUtf8(clientChannel.alloc(), json);
      final DefaultFullHttpRequest request =
          new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, API_TEST + testId, byteBuf);
      request.headers().set("Content-Type", MediaType.APPLICATION_JSON).set("Content-Length", byteBuf.readableBytes());
      clientChannel.writeAndFlush(request);
      while (!future.await(100)) {
        Thread.sleep(10);
      }
      if (clientChannel.closeFuture().isDone()) {
        LOG.warn("Channel closed");
        clientChannel = null;
      }
      if (!future.isSuccess()) {
        if (future.cause() != null) {
          LOG.error(future.cause().getMessage(), future.cause());
        }
        return false;
      }
      return future.get();
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean deleteNetty(@RestPath final String testId) {
    try {
      final Promise<Boolean> future = new DefaultPromise<>(group.next());
      createChannel(hostname, quarkusPort);
      clientChannel.attr(requestTypeAttributeKey).set(RequestType.requestPost(future));
      final DefaultFullHttpRequest request =
          new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.DELETE, API_TEST + testId,
                                     EMPTY_BUFFER);
      clientChannel.writeAndFlush(request);
      while (!future.await(100)) {
        Thread.sleep(10);
      }
      if (clientChannel.closeFuture().isDone()) {
        LOG.warn("Channel closed");
        clientChannel = null;
      }
      if (!future.isSuccess()) {
        if (future.cause() != null) {
          LOG.error(future.cause().getMessage(), future.cause());
        }
        return false;
      }
      return future.get();
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public TestModel getNetty(@RestPath final String testId) {
    try {
      createChannel(hostname, quarkusPort);
      final NettyToInputStream nettyToInputStream = new NettyToInputStream(vertx.getDelegate(), 10);
      clientChannel.attr(requestTypeAttributeKey).set(RequestType.requestGet(nettyToInputStream));
      final DefaultFullHttpRequest request =
          new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, API_TEST + testId, EMPTY_BUFFER);
      request.headers().set("accept", MediaType.APPLICATION_JSON);
      clientChannel.writeAndFlush(request).sync();
      String text = new String(nettyToInputStream.readAllBytes(), StandardCharsets.UTF_8);
      if (clientChannel.closeFuture().isDone()) {
        LOG.warn("Channel closed");
        clientChannel = null;
      }
      return objectMapper.readValue(text, TestModel.class);
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  public void closeWebClient() {
    if (webClient != null) {
      webClient.close();
      webClient = null;
    }
  }

  private void createWebClient() {
    if (webClient == null) {
      WebClientOptions webClientOptions = new WebClientOptions();
      webClientOptions.setDefaultPort(quarkusPort).setDefaultHost(hostname).setMaxChunkSize(BUFFER_SIZE)
                      .setMaxWaitQueueSize(10).setReceiveBufferSize(BUFFER_SIZE).setKeepAlive(true)
                      .setTcpNoDelay(true).setReuseAddress(true).setReusePort(true)
                      .setTryUseCompression(false);
      webClient = WebClient.create(vertx.getDelegate(), webClientOptions);
    }
  }

  public boolean createWebClient(String testId, TestModel testModel) {
    try {
      createWebClient();
      HttpRequest<Buffer> request = webClient.post(API_TEST + testId);
      request.headers().set("Content-Type", MediaType.APPLICATION_JSON);
      final Promise<Boolean> future = new DefaultPromise<>(group.next());
      request.sendJson(testModel).onFailure(throwable -> {
      }).onComplete(httpResponseAsyncResult -> {
        int status = httpResponseAsyncResult.result().statusCode();
        if (httpResponseAsyncResult.succeeded() && status < 400) {
          future.trySuccess(Boolean.TRUE);
        } else {
          Throwable throwable = httpResponseAsyncResult.cause();
          if (throwable != null) {
            future.tryFailure(throwable);
          }
          future.trySuccess(status < 400);
        }
      });
      while (!future.await(100)) {
        Thread.sleep(10);
      }
      if (!future.isSuccess()) {
        if (future.cause() != null) {
          LOG.error(future.cause().getMessage(), future.cause());
        }
        return false;
      }
      return future.get();
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean deleteWebClient(@RestPath final String testId) {
    try {
      createWebClient();
      HttpRequest<Buffer> request = webClient.delete(API_TEST + testId);
      final Promise<Boolean> future = new DefaultPromise<>(group.next());
      request.send().onFailure(throwable -> {
      }).onComplete(httpResponseAsyncResult -> {
        int status = httpResponseAsyncResult.result().statusCode();
        if (httpResponseAsyncResult.succeeded() && status < 400) {
          future.trySuccess(Boolean.TRUE);
        } else {
          Throwable throwable = httpResponseAsyncResult.cause();
          if (throwable != null) {
            future.tryFailure(throwable);
          }
          future.trySuccess(status < 400);
        }
      });
      while (!future.await(100)) {
        Thread.sleep(10);
      }
      if (!future.isSuccess()) {
        if (future.cause() != null) {
          LOG.error(future.cause().getMessage(), future.cause());
        }
        return false;
      }
      return future.get();
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public TestModel getWebClient(@RestPath final String testId) {
    try {
      createWebClient();
      HttpRequest<Buffer> request = webClient.get(API_TEST + testId);
      request.headers().set("accept", MediaType.APPLICATION_JSON);
      final Promise<TestModel> future = new DefaultPromise<>(group.next());
      request.send().onFailure(throwable -> {
      }).onComplete(httpResponseAsyncResult -> {
        int status = httpResponseAsyncResult.result().statusCode();
        if (httpResponseAsyncResult.succeeded() && status < 400) {
          TestModel testModel = httpResponseAsyncResult.result().bodyAsJson(TestModel.class);
          future.trySuccess(testModel);
        } else {
          Throwable throwable = httpResponseAsyncResult.cause();
          if (throwable != null) {
            future.tryFailure(throwable);
          }
          future.trySuccess(null);
        }
      });
      while (!future.await(100)) {
        Thread.sleep(10);
      }
      if (!future.isSuccess()) {
        if (future.cause() != null) {
          LOG.error(future.cause().getMessage(), future.cause());
        }
        return null;
      }
      return future.get();
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  public boolean writeFromInputStreamQuarkus(@RestPath final String testId, final InputStream content) {
    Uni<Response> responseUni = testClientService.createFromInputStream(testId, content);
    try (final Response response = responseUni.await().indefinitely()) {
      if (response == null) {
        // Issue
        LOG.error(RESPONSE_NOT_GET);
        return false;
      }
      if (response.getStatus() >= 400) {
        // Issue
        LOG.error("Status: " + response.getStatus());
        return false;
      }
      return true;
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean writeFromInputStreamWebClient(@RestPath String testId, InputStream content, int port) {
    try {
      WebClientOptions webClientOptions = new WebClientOptions();
      webClientOptions.setDefaultPort(port).setDefaultHost(hostname).setMaxChunkSize(BUFFER_SIZE)
                      .setMaxWaitQueueSize(10).setReceiveBufferSize(BUFFER_SIZE).setKeepAlive(true)
                      .setTcpNoDelay(true).setReuseAddress(true).setReusePort(true)
                      .setTryUseCompression(false);
      final WebClient webClient = WebClient.create(vertx.getDelegate(), webClientOptions);
      HttpRequest<Buffer> request = webClient.post(API_TEST + testId + BINARY);
      request.headers().set("Content-Type", MediaType.APPLICATION_OCTET_STREAM)
             .set("Transfer-Encoding", "chunked");
      final Semaphore semaphore = new Semaphore(0);
      final AtomicBoolean result = new AtomicBoolean(false);
      request.sendStream(
                 new AsyncInputStream(vertx.getDelegate(), vertx.getOrCreateContext().getDelegate(), content))
             .onComplete(res -> {
               if (res.succeeded()) {
                 result.set(true);
                 semaphore.release();
               } else {
                 LOG.error(res.cause(), res.cause());
                 result.set(false);
                 semaphore.release();
               }
               webClient.close();
             }).onFailure(err -> {
               LOG.error(err, err);
               result.set(false);
               semaphore.release();
             });
      semaphore.acquire();
      return result.get();
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean writeFromInputStreamNetty(@RestPath final String testId, final InputStream content,
                                           int port) {
    try {
      final Promise<Boolean> future = new DefaultPromise<>(group.next());
      final Channel channel = clientBootsrap.connect(hostname, port).sync().channel();
      channel.attr(requestTypeAttributeKey).set(RequestType.requestPost(future));
      final DefaultHttpRequest request =
          new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, API_TEST + testId + BINARY);
      HttpUtil.setTransferEncodingChunked(request, true);
      request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE)
             .set("Content-Type", MediaType.APPLICATION_OCTET_STREAM);
      channel.writeAndFlush(request);
      channel.writeAndFlush(new HttpChunkedInput(new ChunkedStream(content, BUFFER_SIZE)));
      // Close the connection after the write operation is done if necessary.
      ChannelFuture close = channel.closeFuture();
      while (!future.await(100)) {
        if (close.isDone()) {
          LOG.error("Should not be");
          break;
        }
        Thread.sleep(10);
      }
      if (!future.isSuccess()) {
        if (future.cause() != null) {
          LOG.error(future.cause().getMessage(), future.cause());
        }
        return false;
      }
      return future.get();
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public InputStream readFromInputStreamQuarkus(@RestPath String testId) {
    Uni<InputStream> responseUni = testClientService.readFromInputStream(testId);
    try {
      final InputStream inputStream = responseUni.await().indefinitely();
      if (inputStream == null) {
        // Issue
        LOG.error(RESPONSE_NOT_GET);
        return null;
      }
      return inputStream;
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  public InputStream readFromInputStreamByteQuarkus(@RestPath String testId) {
    Multi<byte[]> multi = testClientService.readFromInputStreamByte(testId);
    try {
      final BytesToInputStream bytesToInputStream = new BytesToInputStream(vertx.getDelegate());
      final Subscription[] subscriptions = new Subscription[1];
      bytesToInputStream.drainHandler(v -> {
        if (subscriptions[0] != null) {
          subscriptions[0].request(1);
        }
      });
      multi.subscribe(new Subscriber<byte[]>() {
        @Override
        public void onSubscribe(final Subscription subscription) {
          subscriptions[0] = subscription;
          subscription.request(1);
        }

        @Override
        public void onNext(final byte[] bytes) {
          bytesToInputStream.write(bytes);
          if (!bytesToInputStream.writeQueueFull()) {
            subscriptions[0].request(1);
          }
        }

        @Override
        public void onError(final Throwable throwable) {
          LOG.error(throwable.getMessage(), throwable);
          bytesToInputStream.setException(throwable);
          bytesToInputStream.end();
          subscriptions[0].cancel();
        }

        @Override
        public void onComplete() {
          bytesToInputStream.end();
        }
      });
      return bytesToInputStream;
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  public InputStream readFromInputStreamVertx(@RestPath String testId, int port) {
    try {
      final WebClientOptions webClientOptions = new WebClientOptions();
      webClientOptions.setDefaultPort(port).setDefaultHost(hostname).setMaxChunkSize(BUFFER_SIZE)
                      .setMaxWaitQueueSize(10).setReceiveBufferSize(BUFFER_SIZE).setKeepAlive(true)
                      .setTcpNoDelay(true).setReuseAddress(true).setReusePort(true)
                      .setTryUseCompression(false);
      final WebClient webClient = WebClient.create(vertx.getDelegate(), webClientOptions);
      final HttpRequest<Buffer> request = webClient.get(API_TEST + testId + HUGE_BINARY);
      request.headers().set("accept", MediaType.APPLICATION_OCTET_STREAM);
      final VertxToInputStream inputStream = new VertxToInputStream(vertx.getDelegate(), 10);
      request.as(BodyCodec.pipe(inputStream)).send().onFailure(err -> {
        LOG.error(err, err);
        inputStream.setException(err);
      }).onComplete(comp -> {
        LOG.info("Completed: " + comp.succeeded());
        webClient.close();
      });
      return inputStream;
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  public InputStream readFromInputStreamNetty(@RestPath final String testId, int port) {
    try {
      final Channel channel = clientBootsrap.connect(hostname, port).sync().channel();
      final NettyToInputStream nettyToInputStream = new NettyToInputStream(vertx.getDelegate(), 10);
      channel.attr(requestTypeAttributeKey).set(RequestType.requestGet(nettyToInputStream));
      final DefaultFullHttpRequest request =
          new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, API_TEST + testId + HUGE_BINARY,
                                     EMPTY_BUFFER);
      request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE)
             .set("accept", MediaType.APPLICATION_OCTET_STREAM);
      channel.writeAndFlush(request).sync();
      return nettyToInputStream;
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

}
