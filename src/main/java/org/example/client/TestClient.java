package org.example.client;

import static org.example.StaticValues.*;
import static org.example.client.netty.HttpClientHandler.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
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
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.InputStream;
import java.net.URI;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.example.client.netty.HttpClientInitializer;
import org.example.client.netty.RequestType;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TestClient {
  private static final Logger LOG = Logger.getLogger(TestClient.class);
  private static final EventLoopGroup group = new NioEventLoopGroup();
  private static final Bootstrap clientBootsrap;
  private static final String API_TEST = "/api/test/";
  private static final String RESPONSE_NOT_GET = "No Response";

  static {
    clientBootsrap = new Bootstrap();
    clientBootsrap
        .group(group)
        .channel(NioSocketChannel.class)
        .handler(new HttpClientInitializer());
  }

  @RestClient TestClientService testClientServiceNative;
  TestClientService testClientService;
  private String hostname = "localhost";

  public static void closeGlobal() {
    group.shutdownGracefully();
  }

  public TestClient prepare(final String hostname, final int quarkusPort) {
    this.hostname = hostname;
    testClientService =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create("http://" + hostname + ':' + quarkusPort + '/'))
            .build(TestClientService.class);
    return this;
  }

  public boolean writeFromInputStreamQuarkusNative(final String testId, final InputStream content) {
    try {
      final Response response = testClientServiceNative.createFromInputStream(testId, content);
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

  public boolean writeFromInputStreamQuarkus(final String testId, final InputStream content) {
    try {
      final Response response = testClientService.createFromInputStream(testId, content);
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

  public boolean writeFromInputStreamNetty(
      final String testId, final InputStream content, final int port) {
    try {
      final Promise<Boolean> future = new DefaultPromise<>(group.next());
      final Channel channel = clientBootsrap.connect(hostname, port).sync().channel();
      final RequestType requestType = RequestType.requestPost(future);
      channel.attr(requestTypeAttributeKey).set(requestType);
      final DefaultHttpRequest request =
          new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, API_TEST + testId);
      HttpUtil.setTransferEncodingChunked(request, true);
      request
          .headers()
          .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE)
          .set(HttpHeaderNames.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM)
          .set(HttpHeaderNames.HOST, "Fake");
      channel.writeAndFlush(request);
      channel.writeAndFlush(new HttpChunkedInput(new ChunkedStream(content, BUFFER_SIZE)));
      // Close the connection after the write operation is done if necessary.
      final ChannelFuture close = channel.closeFuture();
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

  public InputStream readFromInputStreamQuarkus(final String testId) {
    try {
      return testClientService.readFromInputStream(testId);
    } catch (final Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }
}
