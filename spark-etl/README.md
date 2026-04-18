# Spark ETL Module

Spark Structured Streaming job processing clickstream events from Kafka into aggregated MongoDB collections. Implements three parallel transformation streams for session analytics, page metrics, and user journey tracking.

## Architecture Overview

### Three-Stream Processing Pipeline

```
Kafka (clickstream-events)
    ↓
[JSON Parse + Watermark (10 min late)]
    ↓
    ├─→ Stream 1: Session Aggregates (30-min gap window)
    │   └─→ MongoDB session_aggregates (upsert by sessionId, windowStart)
    │
    ├─→ Stream 2: Page Metrics (5-min tumbling window)
    │   └─→ MongoDB page_metrics (upsert by pageUrl, windowStart) [TTL: 30d]
    │
    └─→ Stream 3: User Journeys (30-min gap window, PAGE_VIEW only)
        └─→ MongoDB user_journeys (upsert by userId, sessionId)
```

## MongoDB Collections Schema

### session_aggregates

**Purpose:** Session-level metrics aggregated in 30-minute gaps

```json
{
  "_id": { "$oid": "..." },
  "sessionId": "sess_123",
  "userId": "user_456",
  "windowStart": { "$date": "2025-04-18T10:00:00.000Z" },
  "windowEnd": { "$date": "2025-04-18T10:30:00.000Z" },
  "durationMs": 1800000,
  "pageViewCount": 12,
  "clickCount": 25,
  "scrollEvents": 8,
  "uniquePages": ["https://example.com/home", "https://example.com/product"],
  "entryPage": "https://example.com/home",
  "exitPage": "https://example.com/product",
  "bounced": false,
  "createdAt": { "$date": "2025-04-18T10:30:05.000Z" }
}
```

**Indexes:**
- Compound: `{ sessionId: 1, windowStart: 1 }` (upsert key)
- Single: `{ userId: 1 }` (query by user)
- Single: `{ windowStart: -1 }` (time range queries)

### page_metrics

**Purpose:** Per-page metrics in 5-minute tumbling windows (auto-expires after 30 days)

```json
{
  "_id": { "$oid": "..." },
  "pageUrl": "https://example.com/product",
  "windowStart": { "$date": "2025-04-18T10:00:00.000Z" },
  "windowEnd": { "$date": "2025-04-18T10:05:00.000Z" },
  "totalViews": 150,
  "uniqueVisitors": 42,
  "clickCount": 85,
  "avgScrollDepth": 0.72,
  "bounceRate": 0.15,
  "createdAt": { "$date": "2025-04-18T10:05:01.000Z" }
}
```

**Indexes:**
- Compound: `{ pageUrl: 1, windowStart: 1 }` (upsert key)
- TTL: `{ createdAt: 1 }` (expires after 2592000 seconds = 30 days)

### user_journeys

**Purpose:** Ordered sequence of pages visited by user within a session

```json
{
  "_id": { "$oid": "..." },
  "userId": "user_456",
  "sessionId": "sess_123",
  "windowStart": { "$date": "2025-04-18T10:00:00.000Z" },
  "windowEnd": { "$date": "2025-04-18T10:30:00.000Z" },
  "orderedPages": [
    {
      "pageUrl": "https://example.com/home",
      "timestamp": { "$date": "2025-04-18T10:00:15.000Z" },
      "clicksOnPage": 2
    },
    {
      "pageUrl": "https://example.com/product",
      "timestamp": { "$date": "2025-04-18T10:05:30.000Z" },
      "clicksOnPage": 5
    }
  ],
  "totalSessionDuration": 1815000,
  "createdAt": { "$date": "2025-04-18T10:30:10.000Z" }
}
```

**Indexes:**
- Compound: `{ userId: 1, sessionId: 1 }` (upsert key)
- Single: `{ userId: 1 }` (query user journeys)

## Configuration

All configuration via [application.yml](./src/main/resources/application.yml). Override via environment variables.

### Kafka Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker endpoints |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Security mode (`PLAINTEXT` or `SASL_SSL`) |
| `KAFKA_SASL_MECHANISM` | (empty) | SASL mechanism if using SASL_SSL |
| `KAFKA_SASL_JAAS_CONFIG` | (empty) | JAAS config string (required for SASL) |

### MongoDB Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGODB_URI` | `mongodb://localhost:27017` | Connection URI |
| `MONGODB_DATABASE` | `clickstream_db` | Database name |

### Spark Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_EXECUTOR_MEMORY` | `512m` | Per-executor heap (dev: 512m, prod: 2g+) |
| `SPARK_DRIVER_MEMORY` | `1g` | Driver heap |
| `SPARK_FORCE_DELETE_CHECKPOINT` | `true` | Force delete checkpoint on restart (dev only) |

### Streaming Parameters

| Setting | Value | Notes |
|---------|-------|-------|
| Trigger | 30 seconds | Micro-batch interval |
| Watermark | 10 minutes | Late event tolerance |
| Session Gap | 30 minutes | Inactivity boundary for session windows |
| Page Window | 5 minutes | Tumbling window for page metrics |
| Checkpoint | `/tmp/spark-checkpoints` | Temp location (dev) or HDFS/S3 (prod) |

## Running the ETL Job

### Development (Embedded Spark)

```bash
# Start infrastructure
cd ..
docker-compose up -d

# Build and run
mvn clean package -f spark-etl/pom.xml
mvn spring-boot:run -f spark-etl/pom.xml
```

**Output:** Spark UI at [http://localhost:4040](http://localhost:4040)

### Production Cluster Deployment

1. **Build fat JAR:**
   ```bash
   mvn clean package -DskipTests -f spark-etl/pom.xml
   ```

2. **Submit to cluster:**
   ```bash
   spark-submit \
     --master yarn \
     --deploy-mode client \
     --executor-cores 4 \
     --executor-memory 2g \
     --driver-memory 1g \
     --conf spark.sql.session.window.buffer.in.memory.threshold=8192 \
     --conf spark.executor.instances=3 \
     spark-etl-1.0.0-SNAPSHOT.jar
   ```

3. **Set environment variables:**
   ```bash
   export KAFKA_BOOTSTRAP_SERVERS="kafka1:9092,kafka2:9092,kafka3:9092"
   export MONGODB_URI="mongodb://replica-set-0:27017,replica-set-1:27017,replica-set-2:27017/?replicaSet=rs0"
   export SPARK_EXECUTOR_MEMORY="4g"
   export SPARK_DRIVER_MEMORY="2g"
   ```

## Monitoring

### Spark UI
- **Dev:** [http://localhost:4040](http://localhost:4040)
- **Prod:** Pushed to YARN/Kubernetes dashboard

### Logs
```bash
# Tail ETL logs
tail -f logs/clickstream-etl.log | grep -E "ERROR|Session|Query"

# Monitor specific stream
tail -f logs/clickstream-etl.log | grep "UserJourneyBuilder"
```

### Metrics to Watch
- **Processing Rate:** Rows/sec in Spark UI (Input Rate)
- **Batch Duration:** Should stay < 30 seconds
- **Event Latency:** Check watermark advances in logs
- **MongoDB Write Lag:** Monitor upsert count per batch

## Troubleshooting

### Checkpoint Corruption

**Error:** `Cannot recover from StateStore`

**Solution:**
```bash
# Dev: Force delete checkpoint
rm -rf /tmp/spark-checkpoints
# Re-run job (will restart from earliest offset)

# Prod: Use checkpoint recovery strategy
# Set: spark.sql.streaming.maxBatchesToRetain = 100
# This keeps 100 batches for recovery
```

### Serialization Issues

**Error:** `NotSerializableException` for MongoClient or SparkSession

**Cause:** Transient dependencies not serializable in closures

**Fix:** Access MongoClient/SparkSession inside `foreachBatch` closure only:
```java
// ✓ CORRECT
ds.writeStream()
  .foreachBatch((batchDf, batchId) -> {
    MongoClient client = createClient();  // Create inside closure
    // Use client...
  })
  .start();

// ✗ WRONG
MongoClient client = createClient();  // Outside closure
ds.writeStream()
  .foreachBatch((batchDf, batchId) -> {
    client.useDatabase(...);  // Not serializable
  })
  .start();
```

### Out of Memory (OOM)

**Error:** `java.lang.OutOfMemoryError: Java heap space`

**Solution:**
1. Increase executor memory: `--executor-memory 4g`
2. Reduce session window buffer: `sql.session.window.buffer.in.memory.threshold: 2048`
3. Partition events by `sessionId` to distribute load:
   ```java
   sparkSession.sql("SELECT * FROM events REPARTITION 32 ")
   ```

### Kafka Offset Reset

**Error:** `Offset out of range` or consumer lag growing

**Solution:**
```bash
# Check current offsets
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group spark-etl-consumer --describe

# Reset to earliest (loss of messages!)
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group spark-etl-consumer --reset-offsets --to-earliest --execute

# Or reset to latest (skip backlog)
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group spark-etl-consumer --reset-offsets --to-latest --execute
```

## Java Module Requirements

Spark 3.5+ on Java 17+ requires explicit module exports:

```
ERROR: module java.base does not export sun.nio.ch to unnamed module
```

**Fix in pom.xml:**
```xml
<jvmArguments>
  --add-exports java.base/sun.nio.ch=ALL-UNNAMED
</jvmArguments>
```

This is configured in the `spring-boot-maven-plugin` in [pom.xml](./pom.xml).

## Code Structure

| Package | Purpose |
|---------|---------|
| `config/` | SparkSession, MongoDB client, streaming monitor beans |
| `job/` | `ClickstreamETLJob` entry point; orchestrates 3 streams |
| `schema/` | `EventSchema` defines Spark SQL DataFrame schemas |
| `transform/` | Stream aggregators: `SessionAggregator`, `PageMetricsAggregator`, `UserJourneyBuilder` |
| `sink/` | `MongoForeachBatchWriter` handles upserts to MongoDB |
| `service/` | `MongoIndexService` creates indexes at startup |

## Dependencies

Key versions (see [pom.xml](./pom.xml)):
- **Spark 3.5.1** (SQL + Kafka connector)
- **MongoDB Driver 5.0+** (async upserts)
- **Jackson 2.17+** (JSON parsing)
- **Spring Boot 3.2.4** (configuration, lifecycle)

## Deployment Checklist

- [ ] Kafka topic `clickstream-events` with 6+ partitions
- [ ] MongoDB `clickstream_db` created, indexes initialized
- [ ] KAFKA_BOOTSTRAP_SERVERS set to production brokers
- [ ] MONGODB_URI set to replica set connection string
- [ ] SPARK_EXECUTOR_MEMORY ≥ 2GB (production)
- [ ] Checkpoint location writable (`/data/spark-checkpoints` or HDFS mounted)
- [ ] JVM module flags present in build configuration
- [ ] Network ACLs allow ETL pod/node to reach Kafka and MongoDB
