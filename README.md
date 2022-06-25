# Test with InputStream and Quarkus

This code is to demonstrate issues with Quarkus and in particular with InpuStream.

Note that in context of Contener, having File or huge memory is not a valid option AFAIK.
InputStream should be a correct way, while consuming only limited memory and no file.

## Goals

Tests show various issues:

- **Grave issue**: When REST API is forwarding an InputStream, default Quarkus implementation of Quarkus 
  client is getting all stream in memory (in fact, twice the InputStream size), which cause Out of Memory 
  issue.
  - Counter measure is to use WebClient and not native Quarkus implemetation.
  - Another counter measure is to use Netty native client (included in Quarkus) with the same result than 
    WebClient but with higher performance (about 15%).

- **Blocking issue**: When REST API is getting an InputStream, default QUarkus implementation of Server side
is not working since it exceeds the limit (10 MB by default) (```Connection was closed: io.vertx.core.
  VertxException: Connection was closed```).
  - Using Netty or WebClient is not enough, Quarkus server is in error due to size exceeding limit.

Trace for Vertx:

```
Request too large: java.io.IOException: Request too large
at org.jboss.resteasy.reactive.server.vertx.VertxInputStream.read(VertxInputStream.java:99)
at org.example.fake.FakeInputStream.consumeAll(FakeInputStream.java:19)
at org.example.server.QuarkusResource.lambda$createFromInputStream$3(QuarkusResource.java:65)
at io.smallrye.context.impl.wrappers.SlowContextualConsumer.accept(SlowContextualConsumer.java:21)
at io.smallrye.mutiny.operators.uni.builders.UniCreateWithEmitter.subscribe(UniCreateWithEmitter.java:22)
at io.smallrye.mutiny.operators.AbstractUni.subscribe(AbstractUni.java:36)
at io.smallrye.mutiny.groups.UniSubscribe.withSubscriber(UniSubscribe.java:52)
at io.smallrye.mutiny.groups.UniSubscribe.with(UniSubscribe.java:112)
at io.smallrye.mutiny.groups.UniSubscribe.with(UniSubscribe.java:89)
at org.jboss.resteasy.reactive.server.handlers.UniResponseHandler.handle(UniResponseHandler.java:17)
at org.jboss.resteasy.reactive.server.handlers.UniResponseHandler.handle(UniResponseHandler.java:8)
at org.jboss.resteasy.reactive.common.core.AbstractResteasyReactiveContext.run(AbstractResteasyReactiveContext.java:141)
at io.quarkus.vertx.core.runtime.VertxCoreRecorder$13.runWith(VertxCoreRecorder.java:543)
at org.jboss.threads.EnhancedQueueExecutor$Task.run(EnhancedQueueExecutor.java:2449)
at org.jboss.threads.EnhancedQueueExecutor$ThreadBody.run(EnhancedQueueExecutor.java:1478)
at org.jboss.threads.DelegatingRunnable.run(DelegatingRunnable.java:29)
at org.jboss.threads.ThreadLocalResettingRunnable.run(ThreadLocalResettingRunnable.java:29)
at io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)
at java.base/java.lang.Thread.run(Thread.java:833)
```

  - When using a Netty implementation of the Server side:
    - Quarkus native client implementation is not possible since Netty configuration is out of Quarkus 
      API (see below).
    - WebClient implementation is OK but twice slower than Netty native client implementation, which is 
      also an issue.
  - Using a Netty implemetation for the Server side does not allow to use the same port than Quarkus 
    REST API service which is obviously not desired.


Final output which shows time and speed (first number is GB/s (higher the better), Memory consumption is a 
simple measure but really closed to the reality).
```
Read Result:
 0.06718598374692876 = Read Client Byte Quarkus (AllInMemory) Server Quarkus Size=500 MB MemConsumption: 204 %
 0.07159757619797393 = Read Client Quarkus (AllInMemory) Server Quarkus Size=500 MB MemConsumption: 203 %
 1.2944739602668789 = Read Client Vertx Server Quarkus Size=10240 MB MemConsumption: 0 %
 1.3129064450721628 = Read Client Netty Server Netty Size=10240 MB MemConsumption: 2 %
 1.4081263242446587 = Read Client Vertx Server Netty Size=10240 MB MemConsumption: 0 %
 1.4753111698837527 = Read Client Netty Server Quarkus Size=10240 MB MemConsumption: 0 %
Write Result:
 1.0037099405194159 = Write Client Vertx Server Netty Size=10240 MB MemConsumption: 2 %
 1.7619637014423413 = Write Client Netty Server Netty Size=10240 MB MemConsumption: 2 %
```