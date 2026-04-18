# Phase 04 — Spark ETL Pipeline

## Context
- Parent: [plan.md](./plan.md)
- Research: [researcher-02-report.md](./research/researcher-02-report.md)
- Demo repo analysis: [researcher-02-report.md](./research/researcher-02-report.md#demo-repo-analysis)
- Depends on: Phase 1 (Docker env), Phase 2 (event schema)

## Overview
- **Priority:** P1
- **Status:** Pending
- **Effort:** 8h
- **Description:** Spark Structured Streaming job consuming from Kafka, transforming raw events into session aggregates, page metrics, and user journey maps, sinking results into MongoDB.

## Key Insights
- Demo repo (`loidinhm31/spring-boot-spark-demo-etl`) uses embedded Spark with Spring Boot managing SparkSession as a bean — reuse this pattern for dev, decouple for prod
- Structured Streaming's `foreachBatch` sink gives full DataFrame API per micro-batch — use MongoClient directly instead of Mongo Spark Connector's streaming mode (more control, fewer bugs)
- Session windows in Spark Structured Streaming: use `session_window()` with 30-min gap
- Spark's internal columnar processing is Arrow-compatible — consistent format across pipeline
- JVM module flags needed: `--add-exports java.base/sun.nio.ch=ALL-UNNAMED`

## Requirements
**Functional:**
- Read from `clickstream-events` Kafka topic as a streaming source
- Produce 3 output collections in MongoDB:
  1. `session_aggregates` — per-session metrics
  2. `page_metrics` — per-page windowed metrics (5-min tumbling window)
  3. `user_journeys` — ordered page sequences per user session
- Handle late data with watermark (10 minutes)

**Non-functional:**
- Micro-batch trigger: 30 seconds (balance latency vs throughput)
- Checkpoint to local filesystem (dev) or HDFS/S3 (prod)
- Idempotent writes to MongoDB (upsert by composite key)

## Architecture

### Data Flow
```
Kafka (clickstream-events)
  → Spark readStream (kafka source)
    → Parse JSON → ClickEvent DataFrame
      → Branch 1: session_window(30min) → SessionAggregate → MongoDB upsert
      → Branch 2: window(5min) group by pageUrl → PageMetric → MongoDB upsert
      → Branch 3: session_window(30min) collect_list → UserJourney → MongoDB upsert
```

### Transformation Details

**1. Session Aggregates (session_window, 30-min gap)**
```
Input:  Raw ClickEvent rows
Group:  session_window(timestamp, "30 minutes"), sessionId, userId
Output: {
  sessionId, userId, windowStart, windowEnd,
  durationMs: max(timestamp) - min(timestamp),
  pageViewCount: count where eventType = PAGE_VIEW,
  clickCount: count where eventType = CLICK,
  scrollEvents: count where eventType = SCROLL,
  uniquePages: collect_set(pageUrl),
  entryPage: first(pageUrl) ordered by timestamp,
  exitPage: last(pageUrl) ordered by timestamp,
  bounced: uniquePages.size == 1 AND durationMs < 10000
}
MongoDB key: { sessionId, windowStart }  (upsert)
```

**2. Page-Level Metrics (5-min tumbling window)**
```
Input:  Raw ClickEvent rows
Group:  window(timestamp, "5 minutes"), pageUrl
Output: {
  pageUrl, windowStart, windowEnd,
  totalViews: count where eventType = PAGE_VIEW,
  uniqueVisitors: approx_count_distinct(userId),
  clickCount: count where eventType = CLICK,
  avgScrollDepth: avg(metadata.scrollDepth) where eventType = SCROLL,
  bounceRate: count(bounced sessions) / count(sessions)
}
MongoDB key: { pageUrl, windowStart }  (upsert)
TTL index: 30 days on windowStart
```

**3. User Journey Maps (session_window, 30-min gap)**
```
Input:  Raw ClickEvent rows filtered to PAGE_VIEW
Group:  session_window(timestamp, "30 minutes"), userId, sessionId
Output: {
  userId, sessionId, windowStart, windowEnd,
  orderedPages: collect_list(struct(pageUrl, timestamp, clicksOnPage)) sorted by timestamp,
  totalSessionDuration: max(timestamp) - min(timestamp)
}
MongoDB key: { userId, sessionId }  (upsert)
```

### Spark Job Structure (adapted from demo repo)

```java
@Configuration
public class SparkConfig {
    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
            .appName("clickstream-etl")
            .master("local[*]")  // dev; remove for cluster submit
            .config("spark.sql.session.window.buffer.in.memory.threshold", 4096)
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
            .getOrCreate();
    }
}

@Service
public class ClickstreamETLJob {
    private final SparkSession spark;
    private final MongoClient mongoClient;

    public void start() {
        Dataset<Row> rawStream = spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "clickstream-events")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", false)
            .load()
            .selectExpr("CAST(value AS STRING) as json")
            .select(from_json(col("json"), eventSchema()).as("event"))
            .select("event.*")
            .withWatermark("timestamp", "10 minutes");

        // Session aggregates stream
        rawStream.groupBy(
                session_window(col("timestamp"), "30 minutes"),
                col("sessionId"), col("userId"))
            .agg(/* aggregations */)
            .writeStream()
            .foreachBatch((batchDf, batchId) -> writeToMongo(batchDf, "session_aggregates"))
            .option("checkpointLocation", "/tmp/spark-checkpoints/sessions")
            .trigger(Trigger.ProcessingTime("30 seconds"))
            .start();

        // Page metrics stream (similar pattern with window())
        // User journeys stream (similar pattern with session_window())
    }

    private void writeToMongo(Dataset<Row> batch, String collection) {
        batch.toLocalIterator().forEachRemaining(row -> {
            Document doc = Document.parse(row.json());
            mongoClient.getDatabase("clickstream_db")
                .getCollection(collection)
                .replaceOne(
                    Filters.eq("_compositeKey", doc.get("_compositeKey")),
                    doc,
                    new ReplaceOptions().upsert(true));
        });
    }
}
```

### Project Structure
```
clickstream-spark-etl/
├── src/main/java/com/clickstream/etl/
│   ├── SparkETLApplication.java
│   ├── config/SparkConfig.java
│   ├── config/MongoConfig.java
│   ├── job/ClickstreamETLJob.java
│   ├── transform/SessionAggregator.java
│   ├── transform/PageMetricsAggregator.java
│   ├── transform/UserJourneyBuilder.java
│   └── sink/MongoForeachBatchWriter.java
├── src/main/resources/application.yml
└── pom.xml
```

### Maven Dependencies
```xml
<dependencies>
    <dependency><groupId>org.apache.spark</groupId><artifactId>spark-sql_2.13</artifactId><version>3.5.x</version></dependency>
    <dependency><groupId>org.apache.spark</groupId><artifactId>spark-sql-kafka-0-10_2.13</artifactId><version>3.5.x</version></dependency>
    <dependency><groupId>org.mongodb</groupId><artifactId>mongodb-driver-sync</artifactId><version>5.x</version></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter</artifactId></dependency>
</dependencies>
```

## Related Code Files
- **Create:** all files in project structure above
- **Reference:** Phase 2 ClickEvent schema (shared model)

## Implementation Steps
1. Create Maven project with Spark + Kafka + MongoDB + Spring Boot dependencies
2. Implement SparkConfig with embedded SparkSession bean
3. Define event schema as StructType matching Phase 2 JSON
4. Implement Kafka readStream with JSON parsing + watermark
5. Implement SessionAggregator transform
6. Implement PageMetricsAggregator transform
7. Implement UserJourneyBuilder transform
8. Implement MongoForeachBatchWriter with upsert logic
9. Wire 3 streaming queries in ClickstreamETLJob.start()
10. Create MongoDB indexes: (sessionId, windowStart), (pageUrl, windowStart), (userId, sessionId)
11. Test with sample events produced to Kafka topic

## Todo
- [ ] Create project skeleton + dependencies
- [ ] Implement SparkConfig
- [ ] Implement event schema + JSON parsing
- [ ] Implement session aggregation transform
- [ ] Implement page metrics transform
- [ ] Implement user journey transform
- [ ] Implement MongoDB foreachBatch writer
- [ ] Wire streaming queries
- [ ] Create MongoDB indexes
- [ ] Integration test with Docker Compose

## Success Criteria
- Streaming job starts and connects to Kafka topic
- Session aggregates appear in MongoDB within 30s of event production
- Page metrics aggregate correctly across 5-min windows
- User journeys show ordered page sequences
- Late events (within 10-min watermark) are included in aggregations
- Job recovers from restart using checkpoints

## Risk Assessment
- **Spark + Spring Boot version conflicts:** Spark bundles its own Guava, Jackson, etc. Use Maven shade or spring-boot-thin-launcher. Demo repo's approach (fat JAR with `--add-exports`) works.
- **Memory pressure with session windows:** Large sessions can buffer many events. Set `spark.sql.session.window.buffer.in.memory.threshold` and monitor.
- **MongoDB write throughput:** foreachBatch serializes per-row. For higher throughput, use `bulkWrite` with `ReplaceOneModel` list.

## Security Considerations
- Spark job connects to Kafka and MongoDB without auth (dev). Prod needs SASL + MongoDB credentials.
- Checkpoint directory must be secure — contains offset state.

## Next Steps
- Phase 3 REST endpoints read from the MongoDB collections this job writes to
- Consider adding Schema Registry for Avro-encoded events (reduces parse errors)
