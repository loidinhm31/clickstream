# System Architecture

Comprehensive technical architecture of the Clickstream Analytics platform.

---

## System Context Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                     CLICKSTREAM ANALYTICS SYSTEM                 │
└──────────────────────────────────────────────────────────────────┘

    Frontend Apps                  Analytics Platform
    (React, Web)                   (Java/Python/Arrow)
          │                               ↓
          │ sendBeacon()          ┌──────────────┐
          └──────────→ Phase 3:   │  Ingestion   │
                      Ingestion   │  API (Java)  │
                      API         └──────┬───────┘
                                         │ Kafka
                        ┌────────────────┼────────────────┐
                        │                │                │
                    Phase 4         Phase 5          Phase 6
                    Spark ETL      Real-time       Raw Archiver
                    (Python)       Analytics       (Java/Spring)
                        │          (Arrow)              │
                    MongoDB         WebSocket       Data Lake
                    Collections     Frontend        (Parquet)
```

---

## Component Architecture

### Layer 1: Ingestion API (Phase 3 — Spring Boot)

**Responsibility:** Accept, validate, and route events to Kafka

```
┌────────────────────────────────────────────────────────┐
│ Spring Boot Application (port 9051)                    │
├────────────────────────────────────────────────────────┤
│ Web Layer                                              │
│ ├── EventController                                    │
│ │   ├── POST /api/events                              │
│ │   └── POST /api/events/batch                        │
│ ├── AnalyticsController                               │
│ │   ├── GET /api/analytics/sessions                   │
│ │   ├── GET /api/analytics/pages                      │
│ │   └── GET /api/analytics/journeys/{userId}          │
│ └── GlobalExceptionHandler                            │
├────────────────────────────────────────────────────────┤
│ Business Layer                                         │
│ ├── EventPublisher (→ Kafka)                          │
│ ├── AnalyticsService (← MongoDB)                      │
│ └── EventValidator                                    │
├────────────────────────────────────────────────────────┤
│ Data Layer                                             │
│ ├── SessionAggregateRepository                        │
│ ├── PageMetricRepository                              │
│ ├── UserJourneyRepository                             │
│ └── Spring Data MongoDB                               │
├────────────────────────────────────────────────────────┤
│ Infrastructure                                         │
│ ├── KafkaProducerConfig                               │
│ ├── MongoIndexConfig                                  │
│ ├── CorsConfig                                        │
│ ├── RateLimitFilter                                   │
│ └── SharedModelConfig                                 │
└────────────────────────────────────────────────────────┘
```

#### Ingestion Flow (POST /api/events)

```
Request: POST /api/events
           ↓
       [CORS Filter]
           ↓
       [RateLimitFilter] ← Check IP rate limit
           ↓ 429 if exceeded
    [EventController.ingest()]
           ↓
    [EventValidator.validate()] ← Check schema
           ↓ 400 if invalid
    [EventPublisher.publishAsync()] → Kafka (async)
           ↓
    Response: 202 Accepted
           ↓
    Kafka Callback: Log success/error (async)
           ↓
    Event in Topic: clickstream-events
```

#### Analytics Query Flow (GET /api/analytics/sessions)

```
Request: GET /api/analytics/sessions?userId=u1&page=0&size=20
           ↓
       [CORS Filter]
           ↓
    [AnalyticsController.getSessions()]
           ↓
    [AnalyticsService.getSessionsByUser()]
           ↓
    [SessionAggregateRepository] → MongoDB Query
           ↓ Filters: {userId, skip, limit}
    MongoDB Returns: Page<SessionAggregate>
           ↓
    Response: 200 OK + JSON
```

---

### Layer 2: Message Broker (Apache Kafka)

**Responsibility:** Distribute events to multiple downstream consumers

```
┌─────────────────────────────────────────────────────┐
│ Apache Kafka Cluster (KRaft mode)                   │
├─────────────────────────────────────────────────────┤
│ Topic: clickstream-events                           │
│ • Partitions: 6                                     │
│ • Replication Factor: 1 (dev) → 3 (prod)           │
│ • Retention: 24h                                    │
│ • Partition Key: sessionId (session affinity)       │
│                                                     │
│ Partition Layout:                                   │
│ ├─ P0: session_001, session_007, session_013, ...  │
│ ├─ P1: session_002, session_008, session_014, ...  │
│ ├─ P2: session_003, session_009, session_015, ...  │
│ ├─ P3: session_004, session_010, session_016, ...  │
│ ├─ P4: session_005, session_011, session_017, ...  │
│ └─ P5: session_006, session_012, session_018, ...  │
│                                                     │
│ Consumer Groups:                                    │
│ ├─ spark-etl: process → aggregate → MongoDB        │
│ ├─ realtime-analytics: stream → Arrow memory       │
│ └─ raw-archiver: buffer → Parquet → data-lake      │
└─────────────────────────────────────────────────────┘
```

**Kafka Producer Config (Ingestion API):**
- `acks=1` (dev) → `acks=all` (prod)
- `compression-type=lz4`
- `batch-size=16KB`, `linger-ms=5`
- Partition key = sessionId (preserves event ordering per session)

---

### Layer 3: Data Layer

#### MongoDB (Primary OLAP Database)

```
┌────────────────────────────────────────────────────────┐
│ MongoDB Replica Set (clickstream_db)                   │
├────────────────────────────────────────────────────────┤
│ Collections (written by Spark, read by API):           │
│                                                        │
│ 1. sessions (indexed)                                 │
│    _id, userId, sessionId, startTime, endTime,        │
│    eventCount, pageViews, clickCount, ...             │
│    Indexes:                                           │
│    • {userId: 1, startTime: -1}                       │
│    • {sessionId: 1} — unique                          │
│    • {startTime: -1} — for TTL                        │
│                                                        │
│ 2. page_metrics (indexed)                             │
│    _id, pageUrl, viewCount, bounceRate,               │
│    avgSessionDuration, topReferrers, ...              │
│    Indexes:                                           │
│    • {pageUrl: 1}                                     │
│    • {lastUpdated: -1}                                │
│                                                        │
│ 3. user_journeys (indexed)                            │
│    _id, userId, sessions: [{sequence, pages: [...]}]  │
│    Indexes:                                           │
│    • {userId: 1}                                      │
│    • {sessionId: 1}                                   │
└────────────────────────────────────────────────────────┘
```

**Connection Pool (Production):**
```
Max Pool Size: 200
Min Pool Size: 50
Max Idle Time: 120s
Connection Lifetime: 30min
→ Supports 40k+ concurrent requests with optimal throughput
```

---

### Layer 4: Downstream Consumers (Planned)

#### Phase 4: Spark ETL (Java/Spring Boot)

**Status:** ✅ Implemented

Spark Structured Streaming job consuming from Kafka, processing events through three parallel aggregation pipelines, and writing results to MongoDB collections.

```
Kafka Consumer (group: spark-etl)
    [ClickstreamETLJob]
         ↓
    Parse JSON + Watermark (10-min late tolerance)
         ↓
    ┌──────────────────────────────────────────┐
    │     THREE PARALLEL AGGREGATION STREAMS   │
    ├──────────────────────────────────────────┤
    │                                          │
    │  Stream 1: Session Aggregates            │
    │  Window: 30-min gap (session_window)     │
    │  Output: SessionAggregator               │
    │  ├─ session_duration                     │
    │  ├─ pageViewCount, clickCount            │
    │  ├─ uniquePages, bounceRate              │
    │  └─ entryPage, exitPage                  │
    │      ↓ MongoDB upsert by (sessionId, ts) │
    │                                          │
    │  Stream 2: Page-Level Metrics            │
    │  Window: 5-min tumbling window           │
    │  Output: PageMetricsAggregator           │
    │  ├─ totalViews, uniqueVisitors           │
    │  ├─ clickCount, avgScrollDepth           │
    │  └─ bounceRate                           │
    │      ↓ MongoDB upsert by (pageUrl, ts)   │
    │      [TTL: 30 days]                      │
    │                                          │
    │  Stream 3: User Journeys                 │
    │  Window: 30-min gap (session_window)     │
    │  Output: UserJourneyBuilder              │
    │  ├─ orderedPages: [url, ts, clicks]      │
    │  └─ totalSessionDuration                 │
    │      ↓ MongoDB upsert by (userId, sid)   │
    │                                          │
    └──────────────────────────────────────────┘
         ↓ MongoForeachBatchWriter
    MongoDB (session_aggregates, page_metrics, user_journeys)
         ↓
    Ready for API queries
```

**Key Architecture Details:**
- **Entry Point:** `SparkETLApplication` (Spring Boot app hosting SparkSession)
- **Job Orchestrator:** `ClickstreamETLJob` (CommandLineRunner, manages three streams)
- **Parallelism:** 3 independent transformations run concurrently on same Kafka stream
- **Sink Pattern:** `foreachBatch()` with `MongoForeachBatchWriter` for idempotent upserts
- **Checkpoint:** Local filesystem (dev) or HDFS/S3 (prod)
- **Trigger:** 30-second micro-batches (balance latency vs throughput)

**Configuration:** See [spark-etl/README.md](../spark-etl/README.md) for:
- MongoDB collection schemas
- Configuration parameters (Kafka, MongoDB, Spark)
- Environment variables for production
- Troubleshooting guide (checkpoint issues, OOM, serialization)
- Deployment instructions (dev, cluster, production)

#### Phase 5: Real-time Analytics (Spring Boot + Apache Arrow)

**Status:** ✅ Implemented

Service that maintains a 15-minute sliding window of Apache Arrow columnar event data and serves real-time metrics via REST API and WebSocket.

**Architecture:**

```
Kafka Consumer (group: realtime-analytics)
         ↓ Batch: up to 500 events/poll
    Apache Arrow Ring Buffer
    ├─ Max 900 batches (15 min @ 1 batch/sec)
    ├─ Columnar VectorSchemaRoot per batch
    └─ Off-heap Netty allocator (512MB default)
         ↓ Every 1.5 seconds
    Compute Metrics:
    ├─ Active Users (5-min window)
    ├─ Click Rate (1-min window)
    ├─ Trending Pages (15-min window)
    └─ Event Rate (1-min window)
         │
         ├─→ HTTP GET /api/realtime/metrics
         │   └─ Arrow IPC binary (on-demand)
         │
         ├─→ WS /ws/realtime/metrics
         │   └─ Arrow IPC frames (push every 1.5s)
         │
         └─→ GET /api/realtime/health
             └─ Kafka consumer status
```

**Key Components:**

1. **MetricsEngine** — Ring buffer management, metric computation
   - Thread-safe ConcurrentLinkedDeque for batch storage
   - Automatic eviction of batches older than 15 minutes
   - Sliding window calculations over Arrow columnar data

2. **EventConsumer** — Kafka listener with batch ack mode
   - Ingests events in batches up to 500 per poll
   - Feeds MetricsEngine via ingestBatch()
   - Health check tracks last successful consume (fails if > 5 min silence)

3. **ArrowIPCSerializer** — Data serialization layer
   - Converts ArrowMetricsSnapshot to Arrow IPC Stream
   - Binary format: platform-agnostic, efficient for network transfer
   - Supports JavaScript (apache-arrow npm), Python (pyarrow), Java (Arrow SDK)

4. **RealtimeMetricsHandler** — WebSocket push mechanism
   - Endpoint: `/ws/realtime/metrics`
   - Rate limiting: max 5 connections per client IP
   - Push interval: 1.5 seconds (configurable)
   - Auto-detects broken connections

5. **RealtimeController** — REST API endpoints
   - GET `/api/realtime/metrics` — Arrow IPC binary (pull-based)
   - GET `/api/realtime/health` — Service health with Kafka status
   - GET `/api/realtime/stats` — Engine statistics (monitoring)

6. **Configuration** — Centralized settings
   - WebSocketConfig, KafkaConsumerConfig, CorsConfig, KafkaConfigValidator
   - Spring CORS configuration applied to both HTTP and WebSocket
   - Allowed origins: configurable from application.yml

**Data Schema (Arrow VectorSchemaRoot):**
```
Events Ring Buffer:
  Field: userId (Utf8)
  Field: sessionId (Utf8)
  Field: eventType (Utf8)
  Field: pageUrl (Utf8)
  Field: timestamp (Int64 epoch millis)
  
Metrics Output:
  Field: activeUsers (Int32)
  Field: clicksPerSecond (Float64)
  Field: eventRate (Float64)
  Field: computedAt (Int64 epoch millis)
  Field: trendingPages (nested struct[])
    └─ pageUrl (Utf8), viewCount (Int32)
```

**Memory Management:**
- Apache Arrow off-heap storage via Netty allocator
- Configurable total memory limit (default 512MB)
- Auto-eviction when ring buffer exceeds 900 batches
- Monitor via GET `/api/realtime/stats` → memoryUsedMB, memoryLimitMB

**Configuration (src/main/resources/application.yml):**
```yaml
server:
  port: 9052

metrics:
  ring-buffer:
    max-batches: 900                 # 900 batches = 15 min @ 1 batch/sec
  windows:
    active-users-seconds: 300        # 5-minute window
    clicks-per-second: 60            # 1-minute window
    trending-pages-seconds: 900      # 15-minute window
    event-rate-seconds: 60           # 1-minute window
  websocket:
    push-interval-ms: 1500           # Push every 1.5 seconds
    allowed-origins:
      - http://localhost:3000
      - http://localhost:9054

arrow:
  allocator:
    limit: 536870912                 # 512MB
```

**API Usage Examples:**

*HTTP Pull (Fallback):*
```bash
curl http://localhost:9052/api/realtime/metrics --output metrics.bin
```

*WebSocket Push (Real-time):*
```javascript
const arrow = require('apache-arrow');
const ws = new WebSocket('ws://localhost:9052/ws/realtime/metrics');
ws.binaryType = 'arraybuffer';
ws.onmessage = (event) => {
  const reader = arrow.RecordBatchReader.from(new Uint8Array(event.data));
  const table = reader.readAll();
  console.log(`Active Users: ${table.getChild('activeUsers').toArray()}`);
};
```

*Health Check:*
```bash
curl http://localhost:9052/api/realtime/health
```

**See Also:** [realtime-analytics/README.md](../realtime-analytics/README.md) for setup, configuration, testing, and troubleshooting.

#### Phase 6: Raw Event Archiver (Java Spring Boot)

**Status:** ✅ COMPLETE

**Responsibility:** Durable long-term archival of raw clickstream events in Parquet columnar format for batch reprocessing, compliance, and data lake analytics.

```
┌──────────────────────────────────────────────────┐
│ Raw Event Archiver (Spring Boot)                 │
├──────────────────────────────────────────────────┤
│ Kafka Consumer (group: raw-archiver-group)       │
│ ├─ Poll: up to 500 events at 6-second intervals  │
│ ├─ Topic: clickstream-events (all 6 partitions)  │
│ └─ Partition key: sessionId affinity             │
├──────────────────────────────────────────────────┤
│ Event Buffer (In-Memory)                         │
│ ├─ Capacity: 10,000 events or 60 seconds (first) │
│ ├─ Thread-safe LinkedList<ClickEvent>            │
│ └─ Flush triggers:                               │
│    1. Buffer reaches 10k events                   │
│    2. 60 seconds elapsed since last flush         │
│    3. Graceful shutdown initiated                │
├──────────────────────────────────────────────────┤
│ Parquet Writer (with Retry & Circuit Breaker)    │
│ ├─ Compression: Snappy (high speed, good ratio)  │
│ ├─ Page Size: 1MB                                │
│ ├─ Row Group Size: 128MB                         │
│ ├─ Schema: 10 fields (eventId, userId, ...)      │
│ └─ On failure (after 3 retries):                 │
│    • Write to error files (data-lake/errors/)    │
│    • Clear buffer to continue processing         │
│    • Log error with timestamp for recovery       │
├──────────────────────────────────────────────────┤
│ Offset Management (Manual Mode)                  │
│ ├─ Acknowledge each event immediately            │
│ ├─ Commit offsets ONLY after successful write    │
│ ├─ On failure: offsets not committed             │
│ └─ Result: No data loss, filtered duplicates     │
├──────────────────────────────────────────────────┤
│ Health Indicator                                 │
│ ├─ Endpoint: GET /actuator/health                │
│ ├─ Detects: Stuck-state (no flush > 5 min)      │
│ ├─ Reports: lastFlushTime, bufferedEventCount    │
│ └─ Status: UP or DOWN (with diagnostic details)  │
└──────────────────────────────────────────────────┘
         ↓
    Data Lake Storage
         ├─ Location: ./data-lake/ (configurable)
         ├─ Path scheme: year/month/day/hour/
         └─ Partitioning by EVENT TIMESTAMP
             (enables efficient range queries)
```

**Architecture Highlights:**

1. **Kafka Consumer** 
   - Manual batch acknowledgment mode
   - 6-second poll interval with 500-event batch size
   - No auto-commit (prevents offset loss before write)

2. **Event Buffer**
   - In-memory LinkedList<ClickEvent>
   - Copy buffer for writing (async I/O outside lock)
   - Thread-safe with minimal lock contention

3. **Parquet Writer**
   - Uses Apache Parquet Java API
   - Snappy compression (fast, ~40% reduction)
   - 128MB row groups for parallel query execution
   - Timestamps stored as epoch milliseconds (Int64)

4. **Offset Management** (Critical for Data Loss Prevention)
   - **Step 1:** Event read from Kafka
   - **Step 2:** Event added to buffer, immediate ack
   - **Step 3:** On flush, batch attempt to write Parquet
   - **Step 4:** After successful write, commit offset
   - **Step 5:** On failure, buffer cleared, offset NOT committed
   - **Result:** No data loss + tolerable duplicates (tunable via batch size)

5. **Circuit Breaker**
   - Retries failed Parquet writes up to 3 times
   - Writes to error directory after exhausting retries
   - Clears buffer to allow new events to process
   - Supports manual recovery of error files

**Data Lake Directory Structure:**

```
data-lake/
├── raw-events/                           # Main partition directory
│   ├── year=2026/
│   │   ├── month=01/
│   │   │   ├── day=01/hour=00/          # First hour of Jan 1
│   │   │   │   ├── part-00001-1704067200000.snappy.parquet
│   │   │   │   ├── part-00002-1704067200000.snappy.parquet
│   │   │   │   └── part-00003-1704067200000.snappy.parquet
│   │   │   ├── day=01/hour=01/
│   │   │   └── day=01/hour=02/
│   │   │
│   │   ├── month=04/
│   │   │   ├── day=17/hour=22/
│   │   │   ├── day=17/hour=23/
│   │   │   ├── day=18/hour=00/
│   │   │   ├── day=18/hour=01/
│   │   │   │   └── part-00001-1713451200000.snappy.parquet
│   │   │   └── ...
│   │   └── month=05/
│   │
│   └── errors/                           # Failed batches for recovery
│       ├── failed-batch-2026-04-18-08-23-45.parquet
│       ├── failed-batch-2026-04-18-10-15-12.parquet
│       └── ...
│
└── _metadata                             # Optional Spark/Trino metadata file
```

**Parquet Schema (10 Fields):**

| Field | Type | Precision | Notes |
|-------|------|-----------|-------|
| eventId | string | 36 (UUID) | Unique event identifier |
| userId | string | 255 | User identifier |
| sessionId | string | 255 | Session identifier |
| eventType | string | 20 | CLICK, PAGE_VIEW, SCROLL, HOVER |
| targetElement | string (nullable) | 500 | HTML selector or element ID |
| pageUrl | string | 2048 | Full page URL |
| referrerUrl | string (nullable) | 2048 | Referrer URL |
| timestamp | long | 19 | Epoch milliseconds (sortable) |
| userAgent | string (nullable) | 500 | Browser user agent |
| schemaVersion | string | 10 | Event schema version (1.0) |

**Configuration (application.yml):**

```yaml
server:
  port: 9053
  shutdown: graceful
  servlet:
    shutdown-wait-time: 30s

spring:
  application:
    name: raw-archiver

kafka:
  bootstrap-servers: localhost:9056
  consumer:
    group-id: raw-archiver-group
    auto-offset-reset: earliest
    enable-auto-commit: false           # Manual offset management
    max-poll-records: 500               # Batch size
    session-timeout-ms: 30000

archiver:
  topic: clickstream-events
  data-lake-base-path: ./data-lake      # Can override with env var
  flush:
    event-threshold: 10000              # Flush after 10k events
    time-interval-seconds: 60           # Or after 60 seconds
  parquet:
    compression: SNAPPY
    page-size: 1048576                  # 1MB
    row-group-size: 134217728           # 128MB
  health:
    stuck-detection-minutes: 5          # DOWN if no flush in 5 min

management:
  endpoints:
    web:
      exposure:
        include: health,metrics
  endpoint:
    health:
      show-details: always
```

**Environment Variables:**

```bash
# Override defaults
export KAFKA_BOOTSTRAP_SERVERS="kafka-prod:9056"
export DATA_LAKE_PATH="/mnt/data-lake"
export FLUSH_EVENT_THRESHOLD="50000"    # Higher for production
export FLUSH_INTERVAL_SECONDS="120"     # Longer for efficiency
```

**Monitoring & Health Endpoint:**

```bash
# Check service status
curl http://localhost:9053/actuator/health

# Response (UP)
{
  "status": "UP",
  "components": {
    "raw-archiver": {
      "status": "UP",
      "details": {
        "lastFlushTime": 1713451920000,
        "bufferedEventCount": 2341,
        "totalEventsProcessed": 1234567,
        "totalEventsArchived": 1234500,
        "lastFlushReason": "EVENT_THRESHOLD"
      }
    },
    "kafkaConsumer": {
      "status": "UP",
      "details": {
        "consumerGroup": "raw-archiver-group",
        "assignedPartitions": 6,
        "lag": 125
      }
    }
  }
}

# Response (DOWN - Stuck State)
{
  "status": "DOWN",
  "components": {
    "raw-archiver": {
      "status": "DOWN",
      "details": {
        "reason": "NO_FLUSH_IN_5_MINUTES",
        "lastFlushTime": 1713451200000,
        "minutesSinceLastFlush": 12,
        "bufferedEventCount": 5678
      }
    }
  }
}
```

**Critical Data Loss Prevention Guarantees:**

✅ **No duplicates if healthy:** Manual offset commit after write succeeds  
✅ **No data loss on failure:** Offset not committed if write fails  
✅ **Graceful degradation:** Errors written to recovery directory  
✅ **Observability:** Health indicator detects stuck states  
✅ **Offset management:** Batch model with immediate ack per event  

**Known Issues & Mitigations:**

1. **Maven Artifactory dependency** 
   - Requires JFrog credentials for build
   - Mitigation: Contact IT for access or disable in settings.xml (dev only)

2. **Concurrent partition processing**
   - Multiple consumer threads writing simultaneously
   - Mitigation: Separate directories per hourly partition (no conflicts)

3. **Disk space monitoring**
   - Data lake can grow large over time
   - Mitigation: Implement retention policies, archive old partitions to S3

4. **Duplicate tolerance**
   - Extreme failures may result in <= N duplicate events 
   - Mitigation: Idempotent schema: `PARTITION BY (year, month, day, hour)`

**See Also:** [raw-archiver/README.md](../raw-archiver/README.md) for complete setup, deployment, and troubleshooting.


---

## Data Flow Architecture

### End-to-End Event Journey

```
1. FRONTEND (React App)
   └─ Click event occurs
   └─ sendBeacon() → POST /api/events (async, fire-and-forget)

2. INGESTION API
   └─ Receive POST /api/events
   └─ Validate event (schema, required fields)
   └─ Publish to Kafka (async)
   └─ Return 202 Accepted

3. KAFKA TOPIC: clickstream-events
   └─ Event partitioned by sessionId (load-balanced across 6 partitions)
   └─ Retained for 24 hours
   └─ Three consumer groups read simultaneously

4. PATH A: SPARK ETL (Consumer 1 — Aggregation)
   └─ Read event from Kafka (group: spark-etl)
   └─ Aggregate: {userId, sessionId, eventCount, pages, bounceRate}
   └─ Window: 30-min gap for session detection
   └─ Write to MongoDB (upsert by sessionId)
   └─ TTL: 90 days (default)

5. PATH B: REAL-TIME ANALYTICS (Consumer 2 — Live Metrics)
   └─ Read event from Kafka (group: realtime-analytics)
   └─ Keep 15-min sliding window in Arrow memory
   └─ Compute: activeUsers, clickRate, event count
   └─ Push via WebSocket every 1.5 seconds to frontend

6. PATH C: RAW ARCHIVER (Consumer 3 — Durability)
   └─ Read event from Kafka (group: raw-archiver-group)
   └─ Buffer in memory (10k events or 60 seconds)
   └─ Write to Parquet file (date-partitioned)
   └─ Manual offset commit after successful write
   └─ Date partitioning: year/month/day/hour/ (event timestamp)

7. ANALYTICS QUERIES (API + Frontend)
   └─ Frontend queries GET /api/analytics/sessions
   └─ API reads from MongoDB (aggregated by Spark)
   └─ Return paginated results with 200ms latency
   
8. LONG-TERM ANALYSIS (Future)
   └─ Spark batch jobs query Parquet files
   └─ Run compliance reports, retention analysis
   └─ Recover data from raw archive if needed
```

**Key Characteristics:**

- **Parallelism:** Three independent consumer groups → no blocking between pipelines
- **Durability:** Raw archiver ensures no data loss with manual offset strategy
- **Latency:** Spark ETL (event-time latency ~30s), Real-time (sub-second push), Raw (eventual write)
- **Storage:** MongoDB (90-day TTL for aggregates), Parquet (indefinite retention by date partition)


---

## Scalability Architecture

### Horizontal Scaling Strategy

```
Single Instance (Development)
┌─────────────┐
│ API Pod 1   │
│ Port 9051   │
└─────┬───────┘
      │ Round-robin
    Kafka

Multiple Instances (Production)
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ API Pod 1   │     │ API Pod 2   │     │ API Pod 3   │
│ Port 9051   │     │ Port 9051   │     │ Port 9051   │
└─────┬───────┘     └─────┬───────┘     └─────┬───────┘
      └─────────────┬─────────────────────────┘
                    │
            ┌───────▼────────┐
            │ Load Balancer  │ (Nginx/K8s)
            │ (Port 80/443)  │
            └────────────────┘
                    │
                  Kafka
```

**Auto-scaling Criteria (Kubernetes):**
- CPU > 70% → Scale up
- Memory > 80% → Scale up
- CPU < 30% for 5min → Scale down
- Min replicas: 3, Max: 10

---

## Performance Characteristics

### Ingestion Path

```
Input: POST /api/events + 1KB JSON body
         ↓ (< 1ms)
     [Validation]
         ↓ (< 1ms)
     [Kafka send async]
         ↓ (< 5ms)
     Response: 202 Accepted

Total Latency: p99 < 10ms
Throughput: 10k+ events/sec (per pod, with connection pooling)
```

### Analytics Query Path

```
Input: GET /api/analytics/sessions?userId=u1&page=0
         ↓ (< 1ms)
     [Construct MongoDB query]
         ↓ (< 5ms)
     [Execute query with index]
         ↓ (< 50ms typical)
     [Serialize JSON response]
         ↓ (< 1ms)
     Response: 200 OK

Total Latency: p99 < 200ms
```

---

## Resilience & Fault Tolerance

### Single Point of Failures (SPOF)

| Component | SPOF Risk | Mitigation |
|-----------|-----------|-----------|
| API | Medium | Horizontal scaling, load balancer |
| Kafka | Low | Replica set (replication factor 3) |
| MongoDB | Low | Replica set (replication factor 3) |
| Network | Low | Multi-AZ/region deployment |

### Failure Scenarios

**Scenario 1: API Pod Crashes**
```
API Pod 1 goes down
    ↓
Health check fails
    ↓
Kubernetes restarts pod (30s)
    ↓
Traffic routed to Pod 2, 3
    ↓
No user-visible impact (transparent failover)
```

**Scenario 2: Kafka Broker Down**
```
Kafka broker 1 fails
    ↓
Replication ensures data on broker 2, 3
    ↓
Producers/consumers auto-reconnect to legal brokers
    ↓
99.9% availability maintained
```

**Scenario 3: MongoDB Connection Pool Exhausted**
```
Connection pool at max (200 connections)
    ↓
New query requests timeout (5s)
    ↓
Return 503 Service Unavailable
    ↓
Client retries or fails gracefully
    ↓
Mitigation: Scale API instances, increase pool
```

---

## Security Architecture

### Authentication & Authorization

**Current:** No authentication (internal network only)

**Planned (Production):**
```
         Client Request
              ↓
    [OAuth 2.0 / JWT Validation]
              ↓ Invalid token → 401
         [Rate Limit Filter]
              ↓ Exceeded → 429
         [API Processing]
              ↓
         [CORS Check]
              ↓ Invalid origin → 403
         Response
```

### Data Privacy (PII)

```
Event Data
    ├─ userId: anonymized (hash)
    ├─ sessionId: random UUID
    ├─ IP address: anonymized (IpAnonymizer)
    ├─ userAgent: kept (for device analytics)
    └─ clicks: kept (for heatmaps)

⚠️ NOT stored:
    • Personal identifiable info (names, emails)
    • Payment information
    • User passwords
```

### Network Security

```
Production Network Topology:

Public Internet
    ↓
[WAF / DDoS Protection]
    ↓
[Load Balancer (HTTPS)]
    ↓
[Internal Network]
    ├─ API Pods
    ├─ Kafka (internal port 9056)
    └─ MongoDB (internal port 9055)

Access Rules:
• Kafka: Only API + Spark + Analytics services
• MongoDB: Only API + Spark services
• API: Public HTTP/HTTPS
```

---

## Monitoring & Observability

### Metrics Collection

```
┌────────────────────────────────┐
│ Spring Boot Metrics            │
│ (Micrometer + Prometheus)      │
├────────────────────────────────┤
│ Endpoint Metrics:              │
│ • http.server.requests         │ (latency, count by endpoint)
│ • http_requests_total          │ (4xx, 5xx errors)
│                                │
│ System Metrics:                │
│ • jvm.memory.used              │ (heap usage)
│ • jvm.thread.count             │ (thread count)
│ • process.cpu.usage            │ (CPU %)
│                                │
│ Kafka Metrics:                 │
│ • kafka.producer.record.lag    │ (async latency)
│ • kafka.producer.records.total │ (success count)
│                                │
│ MongoDB Metrics:               │
│ • mongodb.driver.connection.*  │ (pool stats)
│ • mongodb.driver.query.*       │ (latency)
└────────────────────────────────┘
         ↓ Scrape every 15s
    Prometheus
         ↓
    Grafana Dashboards
    • API Performance
    • Error Rates
    • Resource Utilization
    • Kafka Health
```

### Logging Aggregation

```
Application
    ├─ log level: DEBUG (com.clickstream)
    ├─ log level: INFO (frameworks)
    └─ output: stdout (JSON format)
         ↓
Docker Container
    └─ logs via docker driver
         ↓
Kubernetes
    └─ logs → stdout
         ↓
Fluent Bit
    └─ parse + enrich
         ↓
Elasticsearch
    └─ index: clickstream-ingestion-2026-04-18
         ↓
Kibana
    └─ search, analyze, alert
```

### Alerting Rules

```
Alert: High Error Rate (>1%)
└─ Trigger: 5xx responses > 1% of total
└─ Action: Page on-call engineer

Alert: High Latency (p99 > 50ms)
└─ Trigger: 99th percentile latency > 50ms
└─ Action: Investigate slow queries

Alert: Consumer Lag (> 5 min)
└─ Trigger: Kafka consumer group lag > 50k messages
└─ Action: Scale consumer or investigate performance

Alert: Pod OOMKilled
└─ Trigger: Memory limit exceeded
└─ Action: Increase pod memory limit or disable feature
```

---

## Environment-Specific Configurations

### Development
- Single pod, single Kafka broker, single MongoDB instance
- Auto-index creation enabled
- Logging: DEBUG level
- CORS: http://localhost:3000

### Staging
- 2 API pods behind load balancer
- Kafka cluster (3 brokers), replication factor 3
- MongoDB cluster (3 nodes), replication factor 3
- Logging: INFO level
- CORS: https://staging.example.com

### Production
- 3+ API pods with auto-scaling
- Kafka cluster (3+ brokers), replication factor 3
- MongoDB Atlas or managed cluster, replication factor 3
- Logging: WARN level (Info on demand)
- CORS: https://analytics.example.com
- Rate limiting: Stricter (100 req/s per IP)
---

## Technology Decisions & Rationale

| Decision | Alternative | Why We Chose |
|----------|-----------|------------|
| **Spring Boot** | Django, FastAPI, Express | Java ecosystem, performance, enterprise support |
| **Kafka** | Redis, RabbitMQ, AWS SQS | Durability, distributed, multi-consumer support |
| **MongoDB** | PostgreSQL, DynamoDB | Flexible schema, good analytics queries, horizontal scaling |
| **Async Kafka** | Sync REST write | 202 response + fast ingestion for sendBeacon() |
| **Bucket4j** | Custom rate limiter | Proven, production-ready, token bucket algorithm |
| **Docker Compose** | K8s from start | Simplicity for development, easier onboarding |

---

## Future Architecture Improvements

### Phase 7+: Multi-Region Deployment

```
┌─────────────────┬─────────────────┐
│   US Region     │   EU Region     │
├─────────────────┼─────────────────┤
│ API Cluster 1   │ API Cluster 2   │
│ Kafka Cluster   │ Kafka Cluster   │
│ MongoDB Region 1│ MongoDB Region 2│
└────────┬────────┴────────┬────────┘
         │                │
    Global Load Balancer (GeoDNS)
         │                │
    Frontend Users (USA)  Frontend Users (EU)
```

### Phase 8+: Real-time Streaming Analytics

```
Kafka Events
    ↓
Kafka Streams (Java)
    ├─ Windows (5-min rolling)
    ├─ Aggregations (sum, avg, count)
    └─ Exactly-once semantics
         ↓
In-memory State Store (RocksDB)
         ↓
REST API (analytics/real-time)
         ↓
WebSocket Push
         ↓
Live Dashboard
```

---

## Links to Related Documentation

- [Project Overview & PDR](./project-overview-pdr.md)
- [Code Standards](./code-standards.md)
- [Ingestion API - Configuration](./ingestion-api/configuration.md)
- [Ingestion API - Deployment](./ingestion-api/deployment.md)
