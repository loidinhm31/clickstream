# Phase 02 — Kafka Topic Design & Event Schema

## Context
- Parent: [plan.md](./plan.md)
- Research: [researcher-01-report.md](./research/researcher-01-report.md)

## Overview
- **Priority:** P0 (schema is contract for all consumers)
- **Status:** Pending
- **Effort:** 3h
- **Description:** Define clickstream event schema, Kafka partitioning strategy, retention policy, and consumer group configs.

## Key Insights
- sessionId as partition key distributes load better than userId (avoids hot partitions from power users)
- Session-level ordering within partition enables Spark session windowing without cross-partition joins
- Three consumer groups read independently — Kafka fan-out with no coordination overhead
- JSON schema initially; Avro/Protobuf upgrade path via Schema Registry later

## Requirements
- Single topic `clickstream-events` serving 3 independent consumer groups
- Event schema must support click, page_view, scroll, hover event types
- Partition key ensures session ordering
- Retention long enough for archiver to flush + reprocessing buffer

## Architecture

### Event Schema (JSON)
```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "user-abc-123",
  "sessionId": "sess-xyz-789",
  "eventType": "CLICK",
  "targetElement": "button#submit-order",
  "pageUrl": "https://app.example.com/checkout",
  "referrerUrl": "https://app.example.com/cart",
  "timestamp": 1712678400000,
  "userAgent": "Mozilla/5.0...",
  "metadata": {
    "x": 450,
    "y": 320,
    "scrollDepth": 0.75,
    "viewportWidth": 1920,
    "viewportHeight": 1080,
    "elementText": "Place Order"
  }
}
```

### Event Types
| Type | Trigger | Key Metadata |
|------|---------|-------------|
| `CLICK` | Mouse click on interactive element | x, y, targetElement, elementText |
| `PAGE_VIEW` | Page load / SPA route change | pageUrl, referrerUrl |
| `SCROLL` | Scroll past threshold (25/50/75/100%) | scrollDepth |
| `HOVER` | Hover on element > 500ms | targetElement, durationMs |

### Topic Configuration
```properties
topic.name=clickstream-events
num.partitions=6
replication.factor=1          # dev; 3 for prod
retention.ms=604800000        # 7 days
cleanup.policy=delete         # immutable events, no compaction
compression.type=lz4          # fast compression for high throughput
max.message.bytes=1048576     # 1MB max (events are small ~500B)
```

### Partitioning Strategy: sessionId
**Why sessionId over userId:**
- userId creates hot partitions (power users generate 10-100x more events)
- sessionId is naturally bounded in time (~30 min sessions), distributing evenly
- All events in a session land in same partition → preserves chronological order
- Spark session window aggregation works on single partition without shuffle

**Why not random (null key):**
- Loses ordering guarantees — events from same session scattered across partitions
- Session reconstruction requires expensive cross-partition joins in Spark

**Producer code pattern:**
```java
ProducerRecord<String, String> record = new ProducerRecord<>(
    "clickstream-events",
    event.getSessionId(),  // partition key
    objectMapper.writeValueAsString(event)
);
```

### Consumer Group Configuration

| Consumer Group | Group ID | Concurrency | Offset Reset | Commit Strategy |
|---------------|----------|-------------|-------------|-----------------|
| Spark ETL | `spark-etl-group` | Spark manages (based on partitions) | `earliest` | Spark checkpointing (no auto-commit) |
| Real-time Analytics | `realtime-analytics-group` | 1-2 threads | `latest` | Auto-commit, interval 1000ms |
| Raw Archiver | `raw-archiver-group` | 2-3 threads | `earliest` | Manual commit after Parquet flush |

**Why each choice matters:**
- **Spark ETL** uses `earliest` + checkpointing: exactly-once semantics, reprocessing from last checkpoint on failure
- **Real-time Analytics** uses `latest`: only cares about current data, no need to replay history. Auto-commit OK because losing a few events in a crash doesn't affect sliding window accuracy.
- **Raw Archiver** uses `earliest` + manual commit: must not lose events. Commits only after confirmed Parquet write. On crash, re-reads uncommitted events (idempotent writes to Parquet handle duplicates).

### Kafka's Role: Decoupling, Backpressure, Durability
1. **Decoupling:** Ingestion API doesn't know about or wait for consumers. Add/remove consumers without touching producer. Each consumer group has independent offset tracking.
2. **Backpressure:** If Spark ETL falls behind, events buffer in Kafka (7-day retention). Consumers control their own read pace. Producer never blocks waiting for slow consumers.
3. **Durability:** Events written to disk with configurable retention. Even if all consumers are down, events survive for 7 days. Enables replay and reprocessing.

## Related Code Files
- **Create:** `src/main/java/com/clickstream/model/ClickEvent.java` (shared event POJO)
- **Create:** `src/main/resources/kafka-topic-config.properties`

## Implementation Steps
1. Define `ClickEvent` Java class with all schema fields + Jackson annotations
2. Define `EventMetadata` nested class for the metadata object
3. Create `EventType` enum: CLICK, PAGE_VIEW, SCROLL, HOVER
4. Add input validation: eventId not null, timestamp > 0, eventType valid
5. Write unit tests for serialization/deserialization round-trip
6. Document schema in project README

## Todo
- [ ] Create ClickEvent model class
- [ ] Create EventType enum
- [ ] Create EventMetadata class
- [ ] Add Jackson serialization annotations
- [ ] Write schema validation logic
- [ ] Write unit tests for ser/deser
- [ ] Document schema in README

## Success Criteria
- ClickEvent serializes to JSON matching schema above
- Deserialization handles missing optional fields (metadata subfields)
- EventType enum covers all 4 event types
- Validation rejects events with null eventId or invalid timestamp

## Risk Assessment
- **Schema evolution:** JSON has no built-in schema enforcement. Mitigation: add Kafka Schema Registry (Avro) as future phase.
- **Large metadata payloads:** Scroll events with many data points could grow. Mitigation: max.message.bytes=1MB, validate on ingestion.

## Security Considerations
- Strip PII from metadata before publishing (no email, no passwords)
- userAgent for analytics only, don't store raw IP addresses
- sessionId should be opaque (UUID), not derivable from userId

## Next Steps
- Phase 3 uses this schema for ingestion API request/response
- Phase 4, 5, 6 deserialize this schema from Kafka
