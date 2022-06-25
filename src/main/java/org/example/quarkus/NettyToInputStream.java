package org.example.quarkus;

import io.netty.buffer.ByteBuf;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.streams.WriteStream;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A conversion utility to help move data from a Netty stream to Java classic blocking IO.
 *
 * Use this class to create a {@link WriteStream} that buffers data written to it so that a consumer
 * can use the {@link InputStream} API to read it.
 *
 * The default queue size is 10 writes, but it can be changed using {@link #setWriteQueueMaxSize(int)}
 *
 * @author guss77
 *     Updated and adapted by F.Bregier for Netty
 */
public class NettyToInputStream extends InputStream implements WriteStream<ByteBuf> {
  private static final Logger log = Logger.getLogger(NettyToInputStream.class);
  private final AtomicBoolean inputStreamClosed = new AtomicBoolean(false);
  private final ConcurrentLinkedQueue<PendingWrite> buffer = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<CountDownLatch> readsWaiting = new ConcurrentLinkedQueue<>();
  private final Context context;
  private Handler<Void> drainHandler;
  private Handler<Throwable> errorHandler;
  private int maxSize = 10;
  private Throwable throwable;
  private long readed;
  private final AtomicBoolean writable = new AtomicBoolean(true);

  public NettyToInputStream(final Vertx vertx) {
    context = vertx.getOrCreateContext();
  }

  public NettyToInputStream(final Vertx vertx, final int writeQueueMaxSize) {
    context = vertx.getOrCreateContext();
    setWriteQueueMaxSize(writeQueueMaxSize);
  }

  @Override
  public WriteStream<ByteBuf> setWriteQueueMaxSize(final int maxSize) {
    this.maxSize = maxSize;
    return this;
  }

  public NettyToInputStream wrap(final OutputStream os) {
    context.executeBlocking(p -> {
      try (os) {
        transferTo(os);
        p.complete();
      } catch (Throwable t) {
        p.fail(t);
      }
    }).onFailure(t -> {
      if (errorHandler != null) {
        errorHandler.handle(t);
      }
    });
    return this;
  }

  /* WriteStream stuff */

  /**
   * To set Error when needed for InputStream
   */
  public void setException(final Throwable e) {
    throwable = e;
    if (errorHandler != null) {
      errorHandler.handle(e);
    }
  }

  @Override
  public WriteStream<ByteBuf> drainHandler(final Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }

  @Override
  public void end(final Handler<AsyncResult<Void>> handler) {
    // signal end of stream by writing a null buffer
    if (writable.get()) {
      write(null, handler);
      writable.set(false);
    }
  }

  @Override
  public void write(final ByteBuf data, final Handler<AsyncResult<Void>> handler) {
    write(data).onComplete(handler);
  }

  @Override
  public Future<Void> write(final ByteBuf data) {
    var promise = Promise.<Void>promise();
    if (writable.get()) {
      buffer.add(new PendingWrite(data, promise));
    }
    // flush waiting reads, if any
    for (var l = readsWaiting.poll(); l != null; l = readsWaiting.poll()) {
      l.countDown();
    }
    return promise.future();
  }

  @Override
  public WriteStream<ByteBuf> exceptionHandler(final Handler<Throwable> handler) {
    // we don't have a way to propagate errors as we don't actually handle writing out and InputStream provides no feedback mechanism.
    errorHandler = handler;
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return buffer.size() >= maxSize;
  }

  /**
   * @return current length of already read bytes
   */
  public long getReadLength() {
    return readed;
  }

  /* InputStream stuff */

  @Override
  public int read() throws IOException {
    var wait = 10;
    while (true) {
      if (throwable != null) {
        throw new IOException(throwable);
      }
      while (!buffer.isEmpty() && buffer.peek().shouldDiscard()) {
        buffer.poll();
      }
      if (!buffer.isEmpty()) {
        var read = buffer.peek().readNext();
        if (read > 0) {
          readed += read;
        }
        return read;
      }
      if (inputStreamClosed.get()) {
        return -1;
      }
      // set latch to signal we are waiting
      var latch = new CountDownLatch(1);
      readsWaiting.add(latch);
      if (buffer.isEmpty()) {
        try {
          if (!latch.await(wait, TimeUnit.MILLISECONDS)) {
            readsWaiting.remove(latch);
            wait *= 2;
            if (wait > 10000) {
              throw new IOException("Out of time during read: " + readed);
            }
          } else {
            wait = 10;
          }
        } catch (InterruptedException e) {
          throw new IOException("Failed to wait for data", e);
        }
      }
      // now try to read again
    }
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    if (throwable != null) {
      throw new IOException(throwable);
    }
    if (b.length < off + len) // sanity first
    {
      return 0;
    }
    // we are going to be lazy here and not read more than one pending write, even if there are more available. The contract allows for that
    while (!buffer.isEmpty() && buffer.peek().shouldDiscard()) {
      buffer.poll();
    }
    if (!buffer.isEmpty()) {
      var read = buffer.peek().read(b, off, len);
      if (read > 0) {
        readed += read;
      }
      return read;
    }
    if (inputStreamClosed.get()) {
      return -1;
    }
    // we still wait if there is no data, but let read() implement the blocking
    int val = read();
    if (val < 0) {
      return val;
    }
    b[off] = (byte) (val & 0xFF);
    var nextLen = read(b, off + 1, len - 1);
    if (nextLen < 0) {
      readed++;
      return 1;
    }
    readed += 1 + nextLen;
    return 1 + nextLen;
  }

  @Override
  public long skip(final long len) throws IOException {
    if (throwable != null) {
      throw new IOException(throwable);
    }
    while (!buffer.isEmpty() && buffer.peek().shouldDiscard()) {
      buffer.poll();
    }
    if (!buffer.isEmpty()) {
      var read = buffer.peek().skip(len);
      if (read > 0) {
        readed += read;
      }
      return read;
    }
    if (inputStreamClosed.get()) {
      return -1;
    }
    // we still wait if there is no data, but let read() implement the blocking
    var val = read();
    if (val < 0) {
      return val;
    }
    var nextLen = skip(len - 1);
    if (nextLen < 0) {
      readed++;
      return 1;
    }
    readed += 1 + nextLen;
    return 1 + nextLen;
  }

  @Override
  public int available() throws IOException {
    if (throwable != null) {
      throw new IOException(throwable);
    }
    if (inputStreamClosed.get()) {
      return 0;
    }
    return buffer.stream().map(PendingWrite::available).reduce(0, (i, a) -> i += a);
  }

  @Override
  public void close() throws IOException {
    writable.set(false);
    super.close();
    inputStreamClosed.set(true);
    buffer.clear();
  }

  private class PendingWrite {
    private final ByteBuf data;
    private final Promise<Void> completion;

    private PendingWrite(final ByteBuf data, final Promise<Void> completion) {
      this.data = data;
      this.completion = completion;
      if (data != null) {
        data.retain();
      }
    }

    public boolean shouldDiscard() {
      if (data != null && !data.isReadable()) {
        if (completion.tryComplete()) {
          release();
        }
        if (drainHandler != null) {
          context.runOnContext(drainHandler::handle);
        }
        return true;
      }
      return false;
    }

    private void release() {
      if (data.refCnt() > 0) {
        data.release();
      }
    }

    public int available() {
      return data == null? 0 : data.readableBytes();
    }

    public int readNext() {
      if (data == null) {
        completion.tryComplete();
        inputStreamClosed.set(true);
        return -1;
      }
      int val = 0xFF &
                data.readByte(); // get byte's bitwise value, which is what InputStream#read() is supposed to return
      if (!data.isReadable()) {
        if (completion.tryComplete()) {
          release();
        }
      }
      return val;
    }

    public int read(final byte[] b, final int off, final int len) {
      if (data == null || !data.isReadable()) {
        var completed = completion.tryComplete();
        if (data == null) {
          inputStreamClosed.set(true);
          return -1;
        }
        if (completed) {
          release();
        }
        return 0;
      }
      int max = Math.min(len, data.readableBytes());
      data.readBytes(b, off, max);
      return max;
    }

    public long skip(final long len) {
      if (data == null || !data.isReadable()) {
        var completed = completion.tryComplete();
        if (data == null) {
          inputStreamClosed.set(true);
          return -1;
        }
        if (completed) {
          release();
        }
        return 0;
      }
      return Math.min(len, data.readableBytes());
    }
  }
}
