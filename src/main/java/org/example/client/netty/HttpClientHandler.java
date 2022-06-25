package org.example.client.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Promise;
import org.example.quarkus.NettyToInputStream;
import org.jboss.logging.Logger;

public class HttpClientHandler extends SimpleChannelInboundHandler<HttpObject> {
  public static final AttributeKey<RequestType> requestTypeAttributeKey = AttributeKey.valueOf("RequestType");
  private static final Logger LOG = Logger.getLogger(HttpClientHandler.class);
  private NettyToInputStream nettyToInputStream;
  private Promise<Boolean> future;
  private RequestType requestType;

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final HttpObject msg) throws Exception {
    if (msg instanceof HttpResponse httpResponse) {
      ctx.channel().config().setAutoRead(false);
      requestType = ctx.channel().attr(requestTypeAttributeKey).get();
      if (requestType == null) {
        throw new Exception("RequestType is null");
      }
      if (requestType.isGet()) {
        nettyToInputStream = requestType.getNettyToInputStream();
        nettyToInputStream.drainHandler(v -> ctx.read());
      } else {
        future = requestType.getFuture();
      }
      if (!HttpResponseStatus.OK.equals(httpResponse.status())) {
        Exception exception = new Exception("Issue since response is: " + httpResponse.status());
        if (requestType.isGet()) {
          nettyToInputStream.setException(exception);
          nettyToInputStream.end();
        } else {
          future.setFailure(exception);
        }
        ctx.close();
        throw exception;
      }
    }
    var nextRead = true;
    if (requestType.isGet() && msg instanceof HttpContent content) {
      nettyToInputStream.write(content.content());
      if (nettyToInputStream.writeQueueFull()) {
        nextRead = false;
      }
    }
    if (msg instanceof LastHttpContent) {
      // End but next query?
      nextRead = true;
      if (requestType.isGet()) {
        nettyToInputStream.end();
      } else {
        if (!future.isDone()) {
          future.setSuccess(Boolean.TRUE);
        }
      }
    }
    if (nextRead) {
      ctx.read();
    }
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
    if (requestType != null) {
      if (requestType.getFuture() != null && !requestType.getFuture().isDone()) {
        requestType.getFuture().setFailure(cause);
      }
      if (requestType.getNettyToInputStream() != null) {
        requestType.getNettyToInputStream().setException(cause);
        requestType.getNettyToInputStream().end();
      }
    }
    LOG.error(cause.getMessage(), cause);
    ctx.close();
  }
}
