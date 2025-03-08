package org.example.client.netty;

import static org.example.StaticValues.*;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

public class HttpClientInitializer extends ChannelInitializer<SocketChannel> {

  @Override
  public void initChannel(final SocketChannel ch) {
    final ChannelPipeline p = ch.pipeline();
    p.addLast(new HttpClientCodec(4096, 8192, BUFFER_SIZE));
    p.addLast(new ChunkedWriteHandler());
    p.addLast(new HttpClientHandler());
  }
}
