package org.example;

import io.quarkus.test.junit.QuarkusTest;
import io.vertx.mutiny.core.Vertx;
import org.example.client.TestClient;
import org.example.fake.FakeInputStream;
import org.example.model.TestModel;
import org.example.server.NettyResource;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.example.StaticValues.*;
import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
class QuarkusInputStreamTest {
  private static final Logger LOG = Logger.getLogger(QuarkusInputStreamTest.class);
  private static final int QUARKUS_PORT = 8081;
  private static final int NETTY_PORT = 8083;
  private static final int ITERATION = 100;
  private static long SUPPOSED_LOW_MEM = 100;

  @Inject
  Vertx vertx;

  static SortedMap<Double, String> resultRead = new TreeMap<>();
  static SortedMap<Double, String> resultWrite = new TreeMap<>();

  @BeforeAll
  public static void beforeAll() {
    Runtime runtime = Runtime.getRuntime();
    SUPPOSED_LOW_MEM = (long) ((runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024) * 1.2);
  }

  @AfterAll
  public static void afterAll() {
    StringBuilder builder = new StringBuilder("Results\nRead Result:\n");
    for (Entry<Double, String> entry : resultRead.entrySet()) {
      builder.append("\t").append(entry.getKey()).append(" = ").append(entry.getValue()).append("\n");
    }
    builder.append("Write Result:\n");
    for (Entry<Double, String> entry : resultWrite.entrySet()) {
      builder.append("\t").append(entry.getKey()).append(" = ").append(entry.getValue()).append("\n");
    }
    LOG.info(builder.toString());
    TestClient.closeGlobal();
  }

  final Runtime runtime = Runtime.getRuntime();
  long preMemory;
  long postMemory;

  private long getGcCount() {
    var sum = 0L;
    for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
      var count = b.getCollectionCount();
      if (count != -1) {
        sum += count;
      }
    }
    return sum;
  }

  private long getReallyUsedMemory() throws InterruptedException {
    var before = getGcCount();
    System.gc();
    while (getGcCount() == before) {
      System.gc();
      Thread.sleep(10);
    }
    return getCurrentlyAllocatedMemory();
  }

  private long getReallyUsedMemoryConstraint(long mem) throws InterruptedException {
    for (int i = 0; i < 20; i++) {
      if (getReallyUsedMemory() > mem) {
        Thread.sleep(10);
      } else {
        break;
      }
    }
    return getReallyUsedMemory();
  }

  private long getCurrentlyAllocatedMemory() {
    return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
  }

  private void measure(String name, double duration, long realLen) {
    long realLenMB = realLen / 1024 / 1024;
    LOG.info(name + " Time ms: " + duration + " Speed: " + realLenMB / duration + " Len: " + realLenMB);
    postMemory = getCurrentlyAllocatedMemory();
    LOG.info(postMemory + " MB so " + (postMemory - preMemory) + " MB compare to " + realLenMB + " MB = " +
             (postMemory - preMemory) / realLenMB * 100.0 + " %");
    if (name.startsWith("Read")) {
      resultRead.put(realLenMB / duration, name + " Size=" + realLenMB + " MB MemConsumption: " +
                                           (postMemory - preMemory) * 100 / realLenMB + " %");
    } else {
      resultWrite.put(realLenMB / duration, name + " Size=" + realLenMB + " MB MemConsumption: " +
                                            (postMemory - preMemory) * 100 / realLenMB + " %");
    }

  }

  @BeforeEach
  public void memoryConsumptionBefore() throws InterruptedException {
    getReallyUsedMemoryConstraint(SUPPOSED_LOW_MEM);
    System.gc();
    preMemory = getReallyUsedMemory();
    LOG.info(preMemory + " MB");
  }

  @Test
  void testNonStreamCompare() {
    final TestClient testClient = new TestClient().prepare("localhost", QUARKUS_PORT);
    // Ramp up
    try {
      for (int i = 0; i < ITERATION; i++) {
        assertTrue(testClient.createNetty("test", new TestModel("val1", "val2")),
                   "Error while iteration: " + (i + 1));
        testClient.closeNetty();
        assertTrue(testClient.deleteNetty("test"), "Error while iteration: " + (i + 1));
        testClient.closeNetty();
        TestModel testModel = testClient.getNetty("test");
        assertNotNull(testModel, "Error while iteration: " + (i + 1));
        testClient.closeNetty();
      }
    } finally {
      testClient.closeNetty();
    }
    try {
      for (int i = 0; i < ITERATION; i++) {
        assertTrue(testClient.createNetty("test", new TestModel("val1", "val2")),
                   "Error while iteration: " + (i + 1));
        assertTrue(testClient.deleteNetty("test"), "Error while iteration: " + (i + 1));
        TestModel testModel = testClient.getNetty("test");
        assertNotNull(testModel, "Error while iteration: " + (i + 1));
      }
    } finally {
      testClient.closeNetty();
    }
    try {
      for (int i = 0; i < ITERATION; i++) {
        assertTrue(testClient.createWebClient("test", new TestModel("val1", "val2")),
                   "Error while iteration: " + (i + 1));
        testClient.closeWebClient();
        assertTrue(testClient.deleteWebClient("test"), "Error while iteration: " + (i + 1));
        testClient.closeWebClient();
        TestModel testModel = testClient.getWebClient("test");
        assertNotNull(testModel, "Error while iteration: " + (i + 1));
        testClient.closeWebClient();
      }
    } finally {
      testClient.closeWebClient();
    }
    try {
      for (int i = 0; i < ITERATION; i++) {
        assertTrue(testClient.createWebClient("test", new TestModel("val1", "val2")),
                   "Error while iteration: " + (i + 1));
        assertTrue(testClient.deleteWebClient("test"), "Error while iteration: " + (i + 1));
        TestModel testModel = testClient.getWebClient("test");
        assertNotNull(testModel, "Error while iteration: " + (i + 1));
      }
    } finally {
      testClient.closeWebClient();
    }
    for (int i = 0; i < ITERATION; i++) {
      assertTrue(testClient.create("test", new TestModel("val1", "val2")),
                 "Error while iteration: " + (i + 1));
      TestModel testModel = testClient.get("test");
      assertNotNull(testModel, "Error while iteration: " + (i + 1));
      assertTrue(testClient.delete("test"), "Error while iteration: " + (i + 1));
    }

    // Benchmark
    try {
      long start = System.nanoTime();
      for (int i = 0; i < ITERATION; i++) {
        assertTrue(testClient.createNetty("test", new TestModel("val1", "val2")),
                   "Error while iteration: " + (i + 1));
        testClient.closeNetty();
        assertTrue(testClient.deleteNetty("test"), "Error while iteration: " + (i + 1));
        testClient.closeNetty();
        TestModel testModel = testClient.getNetty("test");
        assertNotNull(testModel, "Error while iteration: " + (i + 1));
        testClient.closeNetty();
      }
      long stop = System.nanoTime();
      LOG.info("Netty Closing Time: " + (stop - start) / 1000000);
    } finally {
      testClient.closeNetty();
    }
    try {
      long start = System.nanoTime();
      for (int i = 0; i < ITERATION; i++) {
        assertTrue(testClient.createNetty("test", new TestModel("val1", "val2")),
                   "Error while iteration: " + (i + 1));
        assertTrue(testClient.deleteNetty("test"), "Error while iteration: " + (i + 1));
        TestModel testModel = testClient.getNetty("test");
        assertNotNull(testModel, "Error while iteration: " + (i + 1));
      }
      long stop = System.nanoTime();
      LOG.info("Netty NoClosing Time: " + (stop - start) / 1000000);
    } finally {
      testClient.closeNetty();
    }
    try {
      long start = System.nanoTime();
      for (int i = 0; i < ITERATION; i++) {
        assertTrue(testClient.createWebClient("test", new TestModel("val1", "val2")),
                   "Error while iteration: " + (i + 1));
        testClient.closeWebClient();
        assertTrue(testClient.deleteWebClient("test"), "Error while iteration: " + (i + 1));
        testClient.closeWebClient();
        TestModel testModel = testClient.getWebClient("test");
        assertNotNull(testModel, "Error while iteration: " + (i + 1));
        testClient.closeWebClient();
      }
      long stop = System.nanoTime();
      LOG.info("WebClient Closing Time: " + (stop - start) / 1000000);
    } finally {
      testClient.closeWebClient();
    }
    try {
      long start = System.nanoTime();
      for (int i = 0; i < ITERATION; i++) {
        assertTrue(testClient.createWebClient("test", new TestModel("val1", "val2")),
                   "Error while iteration: " + (i + 1));
        assertTrue(testClient.deleteWebClient("test"), "Error while iteration: " + (i + 1));
        TestModel testModel = testClient.getWebClient("test");
        assertNotNull(testModel, "Error while iteration: " + (i + 1));
      }
      long stop = System.nanoTime();
      LOG.info("WebClient NoClosing Time: " + (stop - start) / 1000000);
    } finally {
      testClient.closeWebClient();
    }
    long start = System.nanoTime();
    for (int i = 0; i < ITERATION; i++) {
      assertTrue(testClient.create("test", new TestModel("val1", "val2")),
                 "Error while iteration: " + (i + 1));
      TestModel testModel = testClient.get("test");
      assertNotNull(testModel, "Error while iteration: " + (i + 1));
      assertTrue(testClient.delete("test"), "Error while iteration: " + (i + 1));
    }
    long stop = System.nanoTime();
    LOG.info("Quarkus Time: " + (stop - start) / 1000000);
  }

  void writeInputStreamNetty(String name, int port) {
    LOG.info("Start " + name);
    final long realLen = port == QUARKUS_PORT? INPUTSTREAM_SMALL_SIZE : INPUTSTREAM_SIZE;
    long stop;
    long start;
    final TestClient testClient = new TestClient().prepare("localhost", QUARKUS_PORT);
    try (final NettyResource ignored = port == NETTY_PORT? new NettyResource(NETTY_PORT) : null) {
      start = System.nanoTime();
      try (final InputStream inputStream = new FakeInputStream(realLen)) {
        final boolean check = testClient.writeFromInputStreamNetty("test", inputStream, port);
        assertTrue(check);
      } catch (IOException e) {
        fail(e);
      }
      stop = System.nanoTime();
      double duration = (stop - start) / 1000000.0;
      measure(name, duration, realLen);
    } catch (InterruptedException e) {
      fail(e);
    }
  }

  @Test
  void testWriteInputStreamNettyNetty() {
    String name = "Write Client Netty Server Netty";
    writeInputStreamNetty(name, NETTY_PORT);
  }

  @Test
  void testWriteInputStreamNettyQuarkus() {
    String name = "Write Client Netty Server Quarkus";
    writeInputStreamNetty(name, QUARKUS_PORT);
  }

  void writeInputStreamVertx(String name, int port) {
    LOG.info("Start " + name);
    final long realLen = port == QUARKUS_PORT? INPUTSTREAM_SMALL_SIZE : INPUTSTREAM_SIZE;
    long stop;
    long start;
    final TestClient testClient = new TestClient().prepare("localhost", QUARKUS_PORT);
    try (final NettyResource ignored = port == NETTY_PORT? new NettyResource(NETTY_PORT) : null) {
      start = System.nanoTime();
      try (final InputStream inputStream = new FakeInputStream(realLen)) {
        final boolean check = testClient.writeFromInputStreamWebClient("test", inputStream, port);
        assertTrue(check);
      } catch (IOException e) {
        fail(e);
      }
      stop = System.nanoTime();
      double duration = (stop - start) / 1000000.0;
      if (duration > 200) {
        measure(name, duration, realLen);
      } else {
        fail("Time too quick: result incorrect");
      }
    } catch (InterruptedException e) {
      fail(e);
    }
  }

  @Test
  void testWriteInputStreamVertxNetty() {
    String name = "Write Client Vertx Server Netty";
    writeInputStreamVertx(name, NETTY_PORT);
  }

  @Test
  void testWriteInputStreamVertxQuarkus() {
    String name = "Write Client Vertx Server Quarkus";
    writeInputStreamVertx(name, QUARKUS_PORT);
  }

  @Test
  void testWriteInputStreamQuarkusQuarkus() {
    String name = "Write Client Quarkus Server Quarkus";
    LOG.info("Start " + name);
    final long realLen = INPUTSTREAM_SMALL_SIZE;
    long stop;
    long start;
    final TestClient testClient = new TestClient().prepare("localhost", QUARKUS_PORT);
    start = System.nanoTime();
    try (final InputStream inputStream = new FakeInputStream(realLen)) {
      final boolean check = testClient.writeFromInputStreamQuarkus("test", inputStream);
      assertTrue(check);
    } catch (IOException e) {
      fail(e);
    }
    stop = System.nanoTime();
    double duration = (stop - start) / 1000000.0;
    measure(name, duration, realLen);
  }

  void readInputStreamNetty(String name, int port) {
    LOG.info("Start " + name);
    final long realLen = INPUTSTREAM_SIZE;
    long stop;
    long start;
    var total = 0L;
    final TestClient testClient = new TestClient().prepare("localhost", QUARKUS_PORT);
    try (final NettyResource ignored = port == NETTY_PORT? new NettyResource(NETTY_PORT) : null) {
      start = System.nanoTime();
      try (final InputStream inputStream = testClient.readFromInputStreamNetty("dir", port)) {
        total = FakeInputStream.consumeAll(inputStream);
      } catch (IOException e) {
        fail(e);
      }
      stop = System.nanoTime();
      double duration = (stop - start) / 1000000.0;
      assertEquals(realLen, total);
      measure(name, duration, realLen);
    } catch (InterruptedException e) {
      fail(e);
    }
  }

  @Test
  void testReadInputStreamNettyNetty() {
    String name = "Read Client Netty Server Netty";
    readInputStreamNetty(name, NETTY_PORT);
  }

  @Test
  void testReadInputStreamNettyQuarkus() {
    String name = "Read Client Netty Server Quarkus";
    readInputStreamNetty(name, QUARKUS_PORT);
  }

  void readInputStreamVertx(String name, int port) {
    LOG.info("Start " + name);
    final long realLen = INPUTSTREAM_SIZE;
    long stop;
    long start;
    var total = 0L;
    final TestClient testClient = new TestClient().prepare("localhost", QUARKUS_PORT);
    try (final NettyResource ignored = port == NETTY_PORT? new NettyResource(NETTY_PORT) : null) {
      start = System.nanoTime();
      try (final InputStream inputStream = testClient.readFromInputStreamVertx("dir", port)) {
        total = FakeInputStream.consumeAll(inputStream);
      } catch (IOException e) {
        fail(e);
      }
      stop = System.nanoTime();
      double duration = (stop - start) / 1000000.0;
      assertEquals(realLen, total);
      measure(name, duration, realLen);
    } catch (InterruptedException e) {
      fail(e);
    }
  }

  @Test
  void testReadInputStreamVertxNetty() {
    String name = "Read Client Vertx Server Netty";
    readInputStreamVertx(name, NETTY_PORT);
  }

  @Test
  void testReadInputStreamVertxQuarkus() {
    String name = "Read Client Vertx Server Quarkus";
    readInputStreamVertx(name, QUARKUS_PORT);
  }

  @Test
  void testReadInputStreamQuarkusQuarkus() throws InterruptedException {
    String name = "Read Client Quarkus (AllInMemory) Server Quarkus";
    LOG.info("Start " + name);
    final long realLen = INPUTSTREAM_SMALL_SIZE;
    long stop;
    long start;
    var total = 0L;
    final TestClient testClient = new TestClient().prepare("localhost", QUARKUS_PORT);
    start = System.nanoTime();
    try (final InputStream inputStream = testClient.readFromInputStreamQuarkus("dir")) {
      total = FakeInputStream.consumeAll(inputStream);
    } catch (IOException e) {
      fail(e);
    }
    stop = System.nanoTime();
    double duration = (stop - start) / 1000000.0;
    assertEquals(realLen, total);
    measure(name, duration, realLen);
  }

  @Test
  void testReadInputStreamQuarkusByteQuarkus() throws InterruptedException {
    String name = "Read Client Byte Quarkus (AllInMemory) Server Quarkus";
    LOG.info("Start " + name);
    final long realLen = INPUTSTREAM_SMALL_SIZE;
    long stop;
    long start;
    var total = 0L;
    final TestClient testClient = new TestClient().prepare("localhost", QUARKUS_PORT);
    start = System.nanoTime();
    try (final InputStream inputStream = testClient.readFromInputStreamByteQuarkus("dir")) {
      total = FakeInputStream.consumeAll(inputStream);
    } catch (IOException e) {
      fail(e);
    }
    stop = System.nanoTime();
    double duration = (stop - start) / 1000000.0;
    assertEquals(realLen, total);
    measure(name, duration, realLen);
  }
}
