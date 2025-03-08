package org.example.client.netty;

import io.netty.util.concurrent.Promise;

public class RequestType {
  private final Promise<Boolean> future;

  private RequestType(final Promise<Boolean> future) {
    this.future = future;
  }

  public static RequestType requestPost(final Promise<Boolean> future) {
    return new RequestType(future);
  }

  public Promise<Boolean> getFuture() {
    return future;
  }
}
