# Phase 06 — Raw Event Archiver

## Context
- Parent: [plan.md](./plan.md)
- Research: [researcher-01-report.md](./research/researcher-01-report.md#parquet-data-lake-layout)
- Depends on: Phase 1 (Docker env), Phase 2 (event schema)

## Overview
- **Priority:** P2
- **Status:** Pending
- **Effort:** 3h
- **Description:** Simple Kafka consumer writing raw events to Parquet files in a date-partitioned directory layout for long-term archival and future reprocessing.

## Key Insights
- Parquet is Arrow's on-disk counterpart — same columnar format, different storage
- Batch writes (flush every N events or T seconds) for efficient Parquet file sizes
- Manual offset commit after confirmed Parquet write — no data loss
- Simple consumer, no Spark needed — use `parquet-avro` or `arrow-dataset` writer directly

## Requirements
**Functional:**
- Consume all events from `clickstream-events` topic (consumer group: `raw-archiver-group`)
- Write raw events to Parquet files with date-partitioned directory layout
- Flush to new Parquet file every 10,000 events or every 60 seconds (whichever first)
- Commit Kafka offsets only after successful Parquet write

**Non-functional:**
- No data loss — manual offset management
- Parquet files should be 50-200MB for optimal read performance
- Snappy compression for balanced speed/size

## Architecture

### Directory Layout
```
data-lake/
└── raw-events/
    └── year=2026/
        └── month=04/
            └── day=09/
                └── hour=14/
                    ├── part-00000-1712671200.snappy.parquet
                    ├── part-00001-1712671260.snappy.parquet
                    └── ...
```

Hourly partitioning balances:
- File count (not too many small files)
- Query granularity (easy to scan a specific hour)
- Parquet's internal column pruning handles event-type filtering

### Parquet Schema (mirrors ClickEvent)
```
message ClickEvent {
  required binary eventId (UTF8);
  required binary userId (UTF8);
  required binary sessionId (UTF8);
  required binary eventType (UTF8);
  optional binary targetElement (UTF8);
  required binary pageUrl (UTF8);
  optional binary referrerUrl (UTF8);
  required int64 timestamp;
  optional binary userAgent (UTF8);
  optional binary metadata (UTF8);  // JSON string for flexible schema
}
```

### Implementation Pattern
```java
@Service
public class RawEventArchiver {
    private final List<ClickEvent> buffer = new ArrayList<>();
    private final int FLUSH_THRESHOLD = 10_000;
    private final Duration FLUSH_INTERVAL = Duration.ofSeconds(60);
    private Instant lastFlush = Instant.now();

    @KafkaListener(topics = "clickstream-events", groupId = "raw-archiver-group")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        ClickEvent event = objectMapper.readValue(record.value(), ClickEvent.class);
        buffer.add(event);

        if (buffer.size() >= FLUSH_THRESHOLD ||
            Duration.between(lastFlush, Instant.now()).compareTo(FLUSH_INTERVAL) > 0) {
            flushToParquet();
            ack.acknowledge();  // commit offset after successful write
            buffer.clear();
            lastFlush = Instant.now();
        }
    }

    private void flushToParquet() {
        String path = buildPartitionPath(Instant.now());
        // Use Apache Parquet Java writer
        try (ParquetWriter<ClickEvent> writer = AvroParquetWriter.<ClickEvent>builder(new Path(path))
                .withSchema(CLICK_EVENT_SCHEMA)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (ClickEvent event : buffer) {
                writer.write(event);
            }
        }
    }

    private String buildPartitionPath(Instant now) {
        ZonedDateTime zdt = now.atZone(ZoneOffset.UTC);
        return String.format("data-lake/raw-events/year=%d/month=%02d/day=%02d/hour=%02d/part-%05d-%d.snappy.parquet",
            zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth(), zdt.getHour(),
            partCounter.incrementAndGet(), now.getEpochSecond());
    }
}
```

### Kafka Consumer Config
```yaml
spring:
  kafka:
    consumer:
      group-id: raw-archiver-group
      auto-offset-reset: earliest      # don't miss events
      enable-auto-commit: false         # manual commit after Parquet write
      max-poll-records: 1000
    listener:
      ack-mode: manual                  # enables Acknowledgment parameter
```

### Project Structure
```
clickstream-archiver/
├── src/main/java/com/clickstream/archiver/
│   ├── ArchiverApplication.java
│   ├── config/KafkaConsumerConfig.java
│   ├── consumer/RawEventArchiver.java
│   ├── writer/ParquetEventWriter.java
│   └── util/PartitionPathBuilder.java
├── src/main/resources/application.yml
└── pom.xml
```

### Maven Dependencies
```xml
<dependencies>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter</artifactId></dependency>
    <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka</artifactId></dependency>
    <dependency><groupId>org.apache.parquet</groupId><artifactId>parquet-avro</artifactId><version>1.14.x</version></dependency>
    <dependency><groupId>org.apache.hadoop</groupId><artifactId>hadoop-common</artifactId><version>3.4.x</version></dependency>
</dependencies>
```

## Implementation Steps
1. Create Spring Boot project with Kafka + Parquet dependencies
2. Implement PartitionPathBuilder for date-based directory layout
3. Implement ParquetEventWriter using AvroParquetWriter
4. Implement RawEventArchiver Kafka listener with buffer + flush logic
5. Configure manual offset commit
6. Create `data-lake/` directory structure
7. Test: produce events → verify Parquet files appear in correct directories
8. Test: kill archiver mid-batch → restart → verify no events lost (re-reads uncommitted)

## Todo
- [ ] Create project skeleton
- [ ] Implement Parquet writer
- [ ] Implement partition path builder
- [ ] Implement Kafka consumer with manual commit
- [ ] Implement flush logic (count + time threshold)
- [ ] Test data integrity (no loss, no duplicates)
- [ ] Test recovery after crash

## Success Criteria
- Parquet files appear in `data-lake/raw-events/year=.../month=.../day=.../hour=.../`
- Files are valid Parquet (readable by Spark, pandas, DuckDB)
- No events lost on archiver restart (manual commit ensures re-read)
- File sizes are reasonable (not thousands of tiny files)

## Risk Assessment
- **Hadoop dependency bloat:** parquet-avro pulls in hadoop-common (large). Use `hadoop-client-api` minimal JAR or consider Arrow Dataset writer (no Hadoop dependency).
- **Buffer data loss on crash:** Events in buffer are lost. Mitigation: smaller buffer (1000 events) + shorter interval (30s). On restart, Kafka replays from last committed offset.
- **Small files problem:** Low traffic = many small Parquet files. Mitigation: longer flush interval, periodic compaction job.

## Security Considerations
- Data lake directory should have restricted filesystem permissions
- Raw events may contain PII — implement retention policy (auto-delete after N months)
- Consider encryption at rest for compliance

## Next Steps
- Data lake files available for ad-hoc analysis (Spark batch, DuckDB, Athena)
- Future: add compaction job to merge small files
- Future: upload to S3/GCS instead of local filesystem for prod
