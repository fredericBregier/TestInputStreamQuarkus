package org.example.client.netty;

import io.netty.util.concurrent.Promise;
import org.example.quarkus.NettyToInputStream;

public class RequestType {
  private final boolean get;
  private final NettyToInputStream nettyToInputStream;
  private final Promise<Boolean> future;

  private RequestType(final boolean get, final NettyToInputStream nettyToInputStream,
                      final Promise<Boolean> future) {
    this.get = get;
    this.nettyToInputStream = nettyToInputStream;
    this.future = future;
  }

  public static RequestType requestGet(final NettyToInputStream nettyToInputStream) {
    return new RequestType(true, nettyToInputStream, null);
  }

  public static RequestType requestPost(final Promise<Boolean> future) {
    return new RequestType(false, null, future);
  }

  public boolean isGet() {
    return get;
  }

  public NettyToInputStream getNettyToInputStream() {
    return nettyToInputStream;
  }

  public Promise<Boolean> getFuture() {
    return future;
  }
}
