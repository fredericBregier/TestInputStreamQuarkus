package org.example;

import static org.example.StaticValues.*;
import static org.junit.jupiter.api.Assertions.*;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.example.client.TestClient;
import org.example.client.TestClientService;
import org.example.fake.FakeInputStream;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class QuarkusInputStreamTest {
  private static final Logger LOG = Logger.getLogger(QuarkusInputStreamTest.class);
  private static final int QUARKUS_PORT = 8081;
  static SortedMap<Double, String> resultRead = new TreeMap<>();
  static SortedMap<Double, String> resultWrite = new TreeMap<>();
  private static long SUPPOSED_LOW_MEM = 100;
  private static int ITERATION = 20;
  final Runtime runtime = Runtime.getRuntime();
  @RestClient TestClientService testClientService;
  long preMemory;
  long postMemory;

  @BeforeAll
  public static void beforeAll() {
    final Runtime runtime = Runtime.getRuntime();
    SUPPOSED_LOW_MEM =
        (long) ((runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024.0) * 1.2);
  }

  @AfterAll
  public static void afterAll() {
    final StringBuilder builder = new StringBuilder("Results\nRead Result:\n");
    for (final Entry<Double, String> entry : resultRead.entrySet()) {
      builder
          .append("\t")
          .append(entry.getKey())
          .append(" = ")
          .append(entry.getValue())
          .append("\n");
    }
    builder.append("Write Result:\n");
    for (final Entry<Double, String> entry : resultWrite.entrySet()) {
      builder
          .append("\t")
          .append(entry.getKey())
          .append(" = ")
          .append(entry.getValue())
          .append("\n");
    }
    LOG.info(builder.toString());
    TestClient.closeGlobal();
  }

  private long getGcCount() {
    long sum = 0L;
    for (final GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
      final long count = b.getCollectionCount();
      if (count != -1) {
        sum += count;
      }
    }
    return sum;
  }

  private long getReallyUsedMemory() throws InterruptedException {
    final long before = getGcCount();
    System.gc();
    while (getGcCount() == before) {
      System.gc();
      Thread.sleep(100);
    }
    return getCurrentlyAllocatedMemory();
  }

  private void checkReallyUsedMemoryConstraint(final long mem) throws InterruptedException {
    for (int i = 0; i < 20; i++) {
      if (getReallyUsedMemory() > mem) {
        Thread.sleep(10);
      } else {
        break;
      }
    }
    getReallyUsedMemory();
  }

  private long getCurrentlyAllocatedMemory() {
    return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
  }

  private void measure(final String name, final double durationMs, final long realLen) {
    final long realLenMB = realLen / 1024 / 1024;
    LOG.infof(
        "%s Time ms: %f Speed: %f MB/s Len: %d MB",
        name, durationMs, realLenMB / (durationMs / 1000), realLenMB);
    postMemory = getCurrentlyAllocatedMemory();
    LOG.infof(
        "Memory: %d MB so %d MB compare to %d MB = %f percent",
        postMemory,
        (postMemory - preMemory),
        realLenMB,
        (postMemory - preMemory) / (realLenMB * 1.0) * 100.0);
    if (name.startsWith("Read")) {
      resultRead.put(
          realLenMB * 1000 / durationMs,
          name
              + " Size="
              + realLenMB
              + " MB MemConsumption: "
              + (postMemory - preMemory)
              + " Speed: "
              + realLenMB / (durationMs / 1000)
              + " MB/s");
    } else {
      resultWrite.put(
          realLenMB * 1000 / durationMs,
          name
              + " Size="
              + realLenMB
              + " MB MemConsumption: "
              + (postMemory - preMemory)
              + " Speed: "
              + realLenMB / (durationMs / 1000)
              + " MB/s");
    }
  }

  @BeforeEach
  public void memoryConsumptionBefore() throws InterruptedException {
    checkReallyUsedMemoryConstraint(SUPPOSED_LOW_MEM);
    System.gc();
    preMemory = getReallyUsedMemory();
    LOG.infof("%d MB", preMemory);
  }

  void writeInputStreamNetty(final String name, final int port) {
    LOG.infof("Start %s", name);
    final long realLen = INPUTSTREAM_SIZE;
    final long stop;
    final long start;
    final TestClient testClient = new TestClient().prepare("localhost", QUARKUS_PORT);
    start = System.nanoTime();
    try (final InputStream inputStream = new FakeInputStream(realLen)) {
      final boolean check = testClient.writeFromInputStreamNetty("test", inputStream, port);
      assertTrue(check);
    } catch (final IOException e) {
      fail(e);
    }
    stop = System.nanoTime();
    final double duration = (stop - start) / 1000000.0;
    measure(name, duration, realLen);
  }

  @Test
  void testWriteInputStreamNettyQuarkus() {
    final String name = "Write Client Netty Server Quarkus";
    for (int i = 0; i < ITERATION; i++) writeInputStreamNetty(name, QUARKUS_PORT);
  }

  @Test
  void testWriteInputStreamQuarkusQuarkus() {
    for (int i = 0; i < ITERATION; i++) testWriteInputStreamQuarkusQuarkusInternal();
  }

  void testWriteInputStreamQuarkusQuarkusInternal() {
    final String name = "Write Client Quarkus Server Quarkus";
    LOG.infof("Start %s", name);
    final long realLen = INPUTSTREAM_SIZE;
    final long stop;
    final long start;
    final TestClient testClient = new TestClient().prepare("localhost", QUARKUS_PORT);
    start = System.nanoTime();
    try (final InputStream inputStream = new FakeInputStream(realLen)) {
      final boolean check = testClient.writeFromInputStreamQuarkus("test", inputStream);
      assertTrue(check);
    } catch (final IOException e) {
      fail(e);
    }
    stop = System.nanoTime();
    final double duration = (stop - start) / 1000000.0;
    measure(name, duration, realLen);
  }

  @Test
  void testWriteInputStreamQuarkusInjectedQuarkus() {
    for (int i = 0; i < ITERATION; i++) testWriteInputStreamQuarkusInjectedQuarkusInternal();
  }

  void testWriteInputStreamQuarkusInjectedQuarkusInternal() {
    final String name = "Write Client Quarkus Injected Server Quarkus";
    LOG.infof("Start %s", name);
    final long realLen = INPUTSTREAM_SIZE;
    final long stop;
    final long start;
    start = System.nanoTime();
    try (final InputStream inputStream = new FakeInputStream(realLen)) {
      final Response check = testClientService.createFromInputStream("test", inputStream);
      assertTrue(check.getStatus() < 400);
    } catch (final IOException e) {
      fail(e);
    }
    stop = System.nanoTime();
    final double duration = (stop - start) / 1000000.0;
    measure(name, duration, realLen);
  }

  @Test
  void testReadInputStreamQuarkusQuarkus() {
    for (int i = 0; i < ITERATION; i++) testReadInputStreamQuarkusQuarkusInternal();
  }

  void testReadInputStreamQuarkusQuarkusInternal() {
    final String name = "Read Client Quarkus Server Quarkus";
    LOG.infof("Start %s", name);
    final long realLen = INPUTSTREAM_SIZE;
    final long stop;
    final long start;
    long total = 0L;
    final TestClient testClient = new TestClient().prepare("localhost", QUARKUS_PORT);
    start = System.nanoTime();
    try (final InputStream inputStream = testClient.readFromInputStreamQuarkus("dir")) {
      total = FakeInputStream.consumeAll(inputStream);
    } catch (final IOException e) {
      fail(e);
    }
    stop = System.nanoTime();
    final double duration = (stop - start) / 1000000.0;
    assertEquals(realLen, total);
    measure(name, duration, realLen);
  }
}
