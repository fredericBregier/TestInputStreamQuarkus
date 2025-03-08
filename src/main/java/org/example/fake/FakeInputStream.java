package org.example.fake;

import static org.example.StaticValues.*;

import io.smallrye.common.constraint.NotNull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FakeInputStream extends InputStream {
  private static final byte[] EMPTY = new byte[0];
  private long toSend;

  public FakeInputStream(final long len) {
    toSend = len;
  }

  public static long consumeAll(final InputStream inputStream) throws IOException {
    long len = 0;
    int read = 0;
    final byte[] bytes = new byte[BUFFER_SIZE];
    while ((read = inputStream.read(bytes, 0, BUFFER_SIZE)) >= 0) {
      len += read;
    }
    return len;
  }

  @Override
  public int read(@NotNull final byte[] bytes) throws IOException {
    if (toSend <= 0) {
      return -1;
    }
    final int read = (int) Math.min(bytes.length, toSend);
    toSend -= read;
    return read;
  }

  @Override
  public int read(@NotNull final byte[] bytes, final int off, final int len) throws IOException {
    if (toSend <= 0) {
      return -1;
    }
    final int read = (int) Math.min(len - off, toSend);
    toSend -= read;
    return read;
  }

  @Override
  public byte[] readNBytes(final int len) throws IOException {
    if (toSend <= 0) {
      return EMPTY;
    }
    final int read = (int) Math.min(len, toSend);
    final byte[] bytes = new byte[read];
    toSend -= read;
    return bytes;
  }

  @Override
  public int readNBytes(@NotNull final byte[] bytes, final int off, final int len)
      throws IOException {
    if (toSend <= 0) {
      return -1;
    }
    final int read = (int) Math.min(len - off, toSend);
    toSend -= read;
    return read;
  }

  @Override
  public int available() throws IOException {
    return toSend >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) toSend;
  }

  @Override
  public void close() throws IOException {
    toSend = -1;
  }

  @Override
  public int read() throws IOException {
    if (toSend <= 0) {
      return -1;
    }
    toSend--;
    return 'A';
  }

  @Override
  public byte[] readAllBytes() throws IOException {
    if (toSend <= 0) {
      return EMPTY;
    }
    if (toSend >= Integer.MAX_VALUE / 8) {
      throw new OutOfMemoryError("Cannot allocate such array");
    }
    final int read = (int) toSend;
    final byte[] bytes = new byte[read];
    toSend -= read;
    return bytes;
  }

  @Override
  public void skipNBytes(final long n) throws IOException {
    final long read = Math.min(toSend, n);
    toSend -= read;
    if (read < n) {
      throw new IOException("Not enough bytes");
    }
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public long transferTo(@NotNull final OutputStream out) throws IOException {
    final long readFinal = toSend;
    final byte[] bytes = new byte[16 * 1024];
    int read = (int) skip(16 * 1024);
    while (read > 0) {
      out.write(bytes, 0, read);
      read = (int) skip(16 * 1024);
    }
    return readFinal;
  }

  @Override
  public long skip(final long n) throws IOException {
    final long read = Math.min(toSend, n);
    toSend -= read;
    return read;
  }
}
