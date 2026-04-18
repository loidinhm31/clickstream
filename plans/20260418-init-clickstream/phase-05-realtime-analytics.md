# Phase 05 — Real-time Analytics Service (Arrow)

## Context
- Parent: [plan.md](./plan.md)
- Research: [researcher-01-report.md](./research/researcher-01-report.md#arrow-flight-browser-limitation), [researcher-02-report.md](./research/researcher-02-report.md#real-time-analytics-service)

## Overview
- **Priority:** P1
- **Status:** Pending
- **Effort:** 8h
- **Description:** Lightweight Spring Boot service consuming Kafka events, building Apache Arrow columnar tables in-memory, maintaining sliding-window metrics, serving them to the frontend via Arrow IPC over WebSocket.

## Key Insights

### Why Arrow Columnar > Row-Oriented Java Objects
- **Column scan speed:** Computing "clicks per second" scans ONE timestamp column contiguously in memory. With row objects, each `event.getTimestamp()` is a pointer chase to a different heap location.
- **Cache locality:** Same-typed values stored contiguously → entire CPU cache line contains useful data. Row objects scatter fields across heap.
- **Vectorized aggregation:** Arrow compute kernels (sum, count, filter) process entire columns using SIMD instructions — 4-8x throughput vs scalar loops.
- **Memory efficiency:** No per-object overhead (16 bytes header per Java object). Arrow uses flat buffers with null bitmaps.
- **Zero-copy transport:** Same Arrow buffer in memory is the Arrow IPC format — serialize to WebSocket with no row→columnar conversion step.

### Browser Limitation
- Browsers cannot use native gRPC (Arrow Flight's transport). Two options:
  - **HTTP endpoint** returning Arrow IPC binary (`application/octet-stream`) — simpler, polling-based
  - **WebSocket** pushing Arrow IPC frames — true real-time push, lower latency
- **Decision:** WebSocket for real-time push, HTTP fallback for initial load. Both return Arrow IPC bytes.

## Requirements
**Functional:**
- Consume from `clickstream-events` Kafka topic (consumer group: `realtime-analytics-group`)
- Maintain sliding-window metrics:
  1. Active users (last 5 min): count distinct userId
  2. Clicks per second (last 1 min): count CLICK events / 60
  3. Trending pages (last 15 min): top-10 pageUrl by view count
  4. Event rate (last 1 min): total events / 60
- Serve metrics via WebSocket (push every 1-2 seconds)
- Serve metrics via HTTP GET (on-demand pull)
- Both endpoints return Arrow IPC binary

**Non-functional:**
- < 100ms metric computation latency
- < 5MB memory per minute of event data
- Graceful degradation if Kafka is slow (don't block metric serving)

## Architecture

### In-Memory Data Structure
```
Ring buffer of Arrow VectorSchemaRoot batches
  ├── Batch[0]: events from second T-900 to T-899  (oldest, will be evicted)
  ├── Batch[1]: events from second T-899 to T-898
  ├── ...
  └── Batch[899]: events from second T-1 to T       (newest)

Each batch schema:
  - userId: Utf8
  - sessionId: Utf8
  - eventType: Utf8
  - pageUrl: Utf8
  - timestamp: Int64 (epoch ms)
```

### Sliding Window Implementation
```java
public class MetricsEngine {
    private final ConcurrentLinkedDeque<TimestampedBatch> ringBuffer;
    private final BufferAllocator allocator;
    private static final int MAX_WINDOW_SECONDS = 900; // 15 min

    // Called by Kafka consumer thread
    public void ingestBatch(List<ClickEvent> events) {
        VectorSchemaRoot batch = buildArrowBatch(events);
        ringBuffer.addLast(new TimestampedBatch(Instant.now(), batch));
        evictOldBatches();
    }

    // Called by WebSocket/HTTP handler
    public ArrowMetricsSnapshot computeMetrics() {
        long now = System.currentTimeMillis();

        // Active users (5 min window)
        Set<String> activeUsers = new HashSet<>();
        // Clicks per second (1 min window)
        int clickCount = 0;
        // Page views (15 min window)
        Map<String, Integer> pageViews = new HashMap<>();
        int totalEvents = 0;

        for (TimestampedBatch tb : ringBuffer) {
            VectorSchemaRoot root = tb.batch();
            long batchAge = now - tb.timestamp().toEpochMilli();

            for (int i = 0; i < root.getRowCount(); i++) {
                long eventTs = ((BigIntVector) root.getVector("timestamp")).get(i);
                String userId = root.getVector("userId").getObject(i).toString();
                String eventType = root.getVector("eventType").getObject(i).toString();
                String pageUrl = root.getVector("pageUrl").getObject(i).toString();

                if (now - eventTs < 300_000) activeUsers.add(userId);
                if (now - eventTs < 60_000) {
                    totalEvents++;
                    if ("CLICK".equals(eventType)) clickCount++;
                }
                if (now - eventTs < 900_000) {
                    pageViews.merge(pageUrl, 1, Integer::sum);
                }
            }
        }

        return new ArrowMetricsSnapshot(
            activeUsers.size(),
            clickCount / 60.0,
            totalEvents / 60.0,
            topN(pageViews, 10)
        );
    }
}
```

### Arrow IPC Serialization
```java
public byte[] serializeToArrowIPC(ArrowMetricsSnapshot snapshot) {
    try (BufferAllocator alloc = new RootAllocator();
         VectorSchemaRoot root = buildMetricsRoot(alloc, snapshot)) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return out.toByteArray();
    }
}
```

### WebSocket Endpoint
```java
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {
    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(new RealtimeMetricsHandler(metricsEngine), "/ws/realtime/metrics")
                .setAllowedOrigins("http://localhost:3000");
    }
}

public class RealtimeMetricsHandler extends BinaryWebSocketHandler {
    // Scheduled push every 1.5 seconds
    @Scheduled(fixedRate = 1500)
    public void pushMetrics() {
        byte[] arrowIPC = metricsEngine.serializeToArrowIPC(metricsEngine.computeMetrics());
        sessions.forEach(s -> s.sendMessage(new BinaryMessage(arrowIPC)));
    }
}
```

### HTTP Fallback
```java
@RestController
@RequestMapping("/api/realtime")
public class RealtimeController {
    @GetMapping(value = "/metrics", produces = "application/octet-stream")
    public byte[] getMetrics() {
        return metricsEngine.serializeToArrowIPC(metricsEngine.computeMetrics());
    }
}
```

### Project Structure
```
clickstream-realtime/
├── src/main/java/com/clickstream/realtime/
│   ├── RealtimeApplication.java
│   ├── config/
│   │   ├── KafkaConsumerConfig.java
│   │   └── WebSocketConfig.java
│   ├── engine/
│   │   ├── MetricsEngine.java          # Ring buffer + Arrow computation
│   │   ├── TimestampedBatch.java
│   │   └── ArrowMetricsSnapshot.java
│   ├── kafka/
│   │   └── EventConsumer.java          # @KafkaListener → MetricsEngine
│   ├── websocket/
│   │   └── RealtimeMetricsHandler.java # WebSocket push
│   ├── controller/
│   │   └── RealtimeController.java     # HTTP fallback
│   └── serialization/
│       └── ArrowIPCSerializer.java     # VectorSchemaRoot → byte[]
├── src/main/resources/application.yml
└── pom.xml
```

### Maven Dependencies
```xml
<dependencies>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-web</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-websocket</artifactId></dependency>
    <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka</artifactId></dependency>
    <dependency><groupId>org.apache.arrow</groupId><artifactId>arrow-vector</artifactId><version>17.x</version></dependency>
    <dependency><groupId>org.apache.arrow</groupId><artifactId>arrow-memory-netty</artifactId><version>17.x</version></dependency>
    <!-- arrow-flight-core NOT needed — we use Arrow IPC directly -->
</dependencies>
```

**Note:** We use `arrow-vector` + `arrow-memory-netty` for building Arrow tables and serializing to IPC format. We do NOT use `arrow-flight-core` because the browser can't consume native gRPC Flight. Instead, we serialize Arrow IPC bytes and send them over WebSocket/HTTP.

## Implementation Steps
1. Create Spring Boot project with Web, WebSocket, Kafka, Arrow dependencies
2. Implement KafkaConsumerConfig for `realtime-analytics-group` (auto-commit, latest offset)
3. Implement EventConsumer (@KafkaListener) that parses events and feeds MetricsEngine
4. Implement MetricsEngine with ring buffer of Arrow VectorSchemaRoot
5. Implement sliding-window metric computations
6. Implement ArrowIPCSerializer (VectorSchemaRoot → byte[])
7. Implement WebSocket handler with scheduled push (every 1.5s)
8. Implement HTTP fallback controller
9. Configure CORS and WebSocket allowed origins
10. Test with live Kafka events + browser WebSocket client

## Todo
- [ ] Create project skeleton
- [ ] Implement Kafka consumer
- [ ] Implement MetricsEngine ring buffer
- [ ] Implement Arrow batch builder
- [ ] Implement sliding-window computations
- [ ] Implement Arrow IPC serializer
- [ ] Implement WebSocket push handler
- [ ] Implement HTTP fallback
- [ ] Integration test with Kafka + browser

## Success Criteria
- Metrics update within 2 seconds of event production
- WebSocket client receives binary Arrow IPC frames
- Frontend can decode frames with `tableFromIPC()` from `apache-arrow` npm
- Memory usage stays stable under continuous load (ring buffer eviction works)
- HTTP endpoint returns valid Arrow IPC buffer

## Risk Assessment
- **Memory leak from Arrow buffers:** Arrow uses off-heap memory via Netty allocator. Must call `close()` on evicted VectorSchemaRoot batches. Use try-with-resources or explicit eviction in ring buffer.
- **WebSocket disconnection handling:** Track sessions, remove on close, don't push to dead connections.
- **High cardinality for active users:** HashSet of userIds grows with traffic. For very high traffic, switch to HyperLogLog approximation.

## Security Considerations
- WebSocket origin validation (only allow frontend origin)
- No sensitive data in metrics (aggregates only, no raw events)
- Rate limit WebSocket connections per client

## Next Steps
- Phase 7 frontend consumes WebSocket endpoint with `useRealtimeMetrics()` hook
