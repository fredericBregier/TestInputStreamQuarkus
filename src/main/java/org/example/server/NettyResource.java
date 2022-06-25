package org.example.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedStream;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.example.fake.FakeInputStream;
import org.jboss.logging.Logger;

import java.io.InputStream;

import static io.netty.buffer.Unpooled.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;
import static org.example.StaticValues.*;

public class NettyResource implements AutoCloseable {
  private static final Logger log = Logger.getLogger(NettyResource.class);
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final Channel serverChannel;

  public NettyResource(final int port) {
    bossGroup = new NioEventLoopGroup(10);
    workerGroup = new NioEventLoopGroup(100);
    try {
      final ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup);
      b.channel(NioServerSocketChannel.class);
      b.childHandler(new HttpUploadServerInitializer());

      serverChannel = b.bind("localhost", port).sync().channel();
      log.info("Localhost listening on " + port + " channel: " + serverChannel.toString());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws InterruptedException {
    serverChannel.close().sync();
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }

  public static class HttpUploadServerInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    public void initChannel(final SocketChannel ch) {
      final ChannelPipeline pipeline = ch.pipeline();
      pipeline.addLast(new HttpRequestDecoder(4096, 8192, BUFFER_SIZE, true, BUFFER_SIZE, false, true));
      pipeline.addLast(new HttpResponseEncoder());
      pipeline.addLast(new ChunkedWriteHandler());
      pipeline.addLast(new HttpUploadServerHandler());
    }
  }

  public static class HttpUploadServerHandler extends SimpleChannelInboundHandler<HttpObject> {

    final byte[] bytes = new byte[BUFFER_SIZE];
    private HttpRequest request;
    private boolean read;
    private InputStream inputStream;

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final HttpObject msg) throws Exception {
      if (msg instanceof HttpRequest) {
        request = (HttpRequest) msg;
        ctx.channel().config().setAutoRead(false);
        read = request.method() == HttpMethod.GET;
        if (read) {
          inputStream = new FakeInputStream(INPUTSTREAM_SIZE);
        }
      }

      if (msg instanceof LastHttpContent) {
        // Last one so ending chunk
        if (read) {
          final HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
          HttpUtil.setTransferEncodingChunked(response, true);
          var keepAlive = HttpUtil.isKeepAlive(request);
          if (!keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
          }
          ctx.writeAndFlush(response).sync();
          final ChannelFuture writeFuture =
              ctx.writeAndFlush(new HttpChunkedInput(new ChunkedStream(inputStream, BUFFER_SIZE)));
          // Close the connection after the write operation is done if necessary.
          if (!keepAlive) {
            writeFuture.addListener(ChannelFutureListener.CLOSE);
          }
          return;
        }
        writeResponse(ctx.channel());
      } else if (msg instanceof HttpContent chunk) {
        // New chunk is received
        var readable = chunk.content().readableBytes();
        chunk.content().readBytes(bytes, 0, readable);
        ctx.read();
      } else {
        ctx.read();
      }
    }

    private void writeResponse(final Channel channel) {
      writeResponse(channel, false);
    }

    private void writeResponse(final Channel channel, final boolean forceClose) {
      // Decide whether to close the connection or not.
      var keepAlive = HttpUtil.isKeepAlive(request) && !forceClose;

      // Build the response object.
      final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, EMPTY_BUFFER);
      response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);

      if (!keepAlive) {
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
      } else if (request.protocolVersion().equals(HTTP_1_0)) {
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
      }
      // Write the response.
      final ChannelFuture future = channel.writeAndFlush(response);
      // Close the connection after the write operation is done if necessary.
      if (!keepAlive) {
        future.addListener(ChannelFutureListener.CLOSE);
      }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
      log.error(cause.getMessage(), cause);
      ctx.channel().close();
    }
  }

}
