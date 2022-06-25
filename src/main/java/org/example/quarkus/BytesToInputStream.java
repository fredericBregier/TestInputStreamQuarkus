package org.example.quarkus;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.streams.WriteStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A conversion utility to help move data from a Multi byte[] asynchronous stream to Java classic blocking
 * IO.
 *
 * Use this class to create a {@link WriteStream} that buffers data written to it so that a consumer
 * can use the {@link InputStream} API to read it.
 *
 * The default queue size is 10 writes, but it can be changed using {@link #setWriteQueueMaxSize(int)}
 *
 * @author guss77
 *     Updated and adapted by F.Bregier but not as efficient as byte[] version
 */
public class BytesToInputStream extends InputStream implements WriteStream<byte[]> {

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ConcurrentLinkedQueue<PendingWrite> buffer = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<CountDownLatch> readsWaiting = new ConcurrentLinkedQueue<>();
  private final Context context;
  private Handler<Void> drainHandler;
  private Handler<Throwable> errorHandler;
  private int maxSize = 10;
  private Throwable throwable;

  public BytesToInputStream(Vertx vertx) {
    context = vertx.getOrCreateContext();
  }

  public BytesToInputStream(Vertx vertx, int writeQueueMaxSize) {
    context = vertx.getOrCreateContext();
    setWriteQueueMaxSize(writeQueueMaxSize);
  }

  @Override
  public WriteStream<byte[]> setWriteQueueMaxSize(int maxSize) {
    this.maxSize = maxSize;
    return this;
  }

  public BytesToInputStream wrap(OutputStream os) {
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

  public void setException(final Throwable e) {
    throwable = e;
  }

  @Override
  public WriteStream<byte[]> drainHandler(Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    // signal end of stream by writing a null buffer
    write(null, handler);
  }

  @Override
  public void write(byte[] data, Handler<AsyncResult<Void>> handler) {
    write(data).onComplete(handler);
  }

  @Override
  public Future<Void> write(byte[] data) {
    var promise = Promise.<Void>promise();
    buffer.add(new PendingWrite(data, promise));
    // flush waiting reads, if any
    for (var l = readsWaiting.poll(); l != null; l = readsWaiting.poll()) {
      l.countDown();
    }
    return promise.future();
  }

  @Override
  public WriteStream<byte[]> exceptionHandler(Handler<Throwable> handler) {
    // we don't have a way to propagate errors as we don't actually handle writing out and InputStream provides no feedback mechanism.
    errorHandler = handler;
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return buffer.size() >= maxSize;
  }

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
        return buffer.peek().readNext();
      }
      if (closed.get()) {
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
              throw new IOException("Out of time during read");
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

  /* InputStream stuff */

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
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
      return buffer.peek().read(b, off, len);
    }
    if (closed.get()) {
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
      return 1;
    }
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
      return buffer.peek().skip(len);
    }
    if (closed.get()) {
      return -1;
    }
    // we still wait if there is no data, but let read() implement the blocking
    int val = read();
    if (val < 0) {
      return val;
    }
    var nextLen = skip(len - 1);
    if (nextLen < 0) {
      return 1;
    }
    return 1 + nextLen;
  }

  @Override
  public int available() throws IOException {
    if (throwable != null) {
      throw new IOException(throwable);
    }
    if (closed.get()) {
      return 0;
    }
    return buffer.stream().map(PendingWrite::available).reduce(0, (i, a) -> i += a);
  }

  @Override
  public void close() throws IOException {
    super.close();
    closed.set(true);
    buffer.clear();
  }

  private class PendingWrite {
    private final byte[] data;
    private final Promise<Void> completion;
    private int position;

    private PendingWrite(byte[] data, Promise<Void> completion) {
      this.data = data;
      this.completion = completion;
    }

    public boolean shouldDiscard() {
      if (data != null && position >= data.length) {
        completion.tryComplete();
        if (drainHandler != null) {
          context.runOnContext(drainHandler::handle);
        }
        return true;
      }
      return false;
    }

    public int available() {
      return data == null? 0 : data.length;
    }

    public int readNext() {
      if (data == null) {
        completion.tryComplete();
        closed.set(true);
        return -1;
      }
      int val = 0xFF &
                data[position++]; // get byte's bitwise value, which is what InputStream#read() is supposed to return
      if (position >= data.length) {
        completion.tryComplete();
      }
      return val;
    }

    public int read(byte[] b, int off, int len) {
      if (data == null || position >= data.length) {
        completion.tryComplete();
        if (data == null) {
          closed.set(true);
          return -1;
        }
        return 0;
      }
      int max = Math.min(len, data.length - position);
      System.arraycopy(data, position, b, off, max);
      position += max;
      return max;
    }

    public long skip(long len) {
      if (data == null || position >= data.length) {
        completion.tryComplete();
        if (data == null) {
          closed.set(true);
          return -1;
        }
        return 0;
      }
      long max = Math.min(len, data.length - position);
      position += max;
      return max;
    }
  }
}
