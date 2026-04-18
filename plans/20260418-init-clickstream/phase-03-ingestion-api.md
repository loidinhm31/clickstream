# Phase 03 — Spring Boot Ingestion API

## Context
- Parent: [plan.md](./plan.md)
- Depends on: Phase 1 (Docker env), Phase 2 (event schema)

## Overview
- **Priority:** P1
- **Status:** Pending
- **Effort:** 6h
- **Description:** Spring Boot service with dual responsibility: (1) receive click events via HTTP POST → publish to Kafka, (2) serve historical analytics from MongoDB via REST.

## Key Insights
- Single Spring Boot app handles both ingestion and REST queries — simplicity for solo dev
- KafkaTemplate with sessionId key ensures partition affinity
- `sendBeacon` from frontend is fire-and-forget — API must be fast, async Kafka send
- MongoDB queries are read-only — data written by Spark ETL, read by this service

## Requirements
**Functional:**
- `POST /api/events` — accept single event, validate, publish to Kafka, return 202 Accepted
- `POST /api/events/batch` — accept array of events (up to 100), publish all, return 202
- `GET /api/analytics/sessions` — query session aggregates from MongoDB (with filters: userId, dateRange, pagination)
- `GET /api/analytics/pages` — query page metrics from MongoDB (with filters: pageUrl, dateRange)
- `GET /api/analytics/journeys/{userId}` — query user journey maps

**Non-functional:**
- Event ingestion < 10ms p99 latency (async Kafka send)
- REST queries < 200ms p99 (indexed MongoDB queries)
- CORS enabled for frontend origin

## Architecture

### Project Structure
```
clickstream-ingestion/
├── src/main/java/com/clickstream/
│   ├── ClickstreamApplication.java
│   ├── config/
│   │   ├── KafkaProducerConfig.java
│   │   └── MongoConfig.java
│   ├── controller/
│   │   ├── EventController.java          # POST /api/events
│   │   └── AnalyticsController.java      # GET /api/analytics/*
│   ├── model/
│   │   ├── ClickEvent.java               # Shared event schema
│   │   ├── EventType.java
│   │   ├── EventMetadata.java
│   │   ├── SessionAggregate.java         # MongoDB read model
│   │   ├── PageMetric.java
│   │   └── UserJourney.java
│   ├── service/
│   │   ├── EventPublisher.java           # KafkaTemplate wrapper
│   │   └── AnalyticsService.java         # MongoDB query service
│   ├── repository/
│   │   ├── SessionAggregateRepository.java
│   │   ├── PageMetricRepository.java
│   │   └── UserJourneyRepository.java
│   └── validation/
│       └── EventValidator.java
├── src/main/resources/
│   └── application.yml
└── pom.xml
```

### Key Code Patterns

**EventController (ingestion):**
```java
@RestController
@RequestMapping("/api/events")
public class EventController {
    private final EventPublisher publisher;

    @PostMapping
    public ResponseEntity<Void> ingest(@Valid @RequestBody ClickEvent event) {
        publisher.publishAsync(event);
        return ResponseEntity.accepted().build();  // 202 — fire and forget
    }

    @PostMapping("/batch")
    public ResponseEntity<Void> ingestBatch(@RequestBody List<@Valid ClickEvent> events) {
        events.forEach(publisher::publishAsync);
        return ResponseEntity.accepted().build();
    }
}
```

**EventPublisher (Kafka producer):**
```java
@Service
public class EventPublisher {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;

    public void publishAsync(ClickEvent event) {
        String key = event.getSessionId();  // partition by session
        String value = mapper.writeValueAsString(event);
        kafka.send("clickstream-events", key, value)
             .whenComplete((result, ex) -> {
                 if (ex != null) log.error("Kafka send failed", ex);
             });
    }
}
```

**application.yml:**
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: 1               # leader ack (fast); use 'all' for prod
      compression-type: lz4
      batch-size: 16384
      linger-ms: 5           # small batch window for low latency
  data:
    mongodb:
      uri: mongodb://localhost:27017/clickstream_db

server:
  port: 8081  # 8080 taken by Kafka UI
```

### Maven Dependencies (pom.xml key entries)
```xml
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.kafka</groupId>
        <artifactId>spring-kafka</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-data-mongodb</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-validation</artifactId>
    </dependency>
</dependencies>
```

## Related Code Files
- **Create:** all files in project structure above
- **Modify:** `docker-compose.yml` (optional: add service entry for containerized deployment)

## Implementation Steps
1. Initialize Spring Boot project (Spring Initializr: Web, Kafka, MongoDB, Validation)
2. Create shared model classes from Phase 2 schema
3. Implement `EventPublisher` with async KafkaTemplate
4. Implement `EventController` with POST endpoints + validation
5. Create MongoDB document models (SessionAggregate, PageMetric, UserJourney)
6. Implement Spring Data MongoDB repositories
7. Implement `AnalyticsService` with pagination, filtering, date range queries
8. Implement `AnalyticsController` with GET endpoints
9. Configure CORS for frontend origin (`http://localhost:3000`)
10. Write integration tests with embedded Kafka + testcontainers

## Todo
- [ ] Initialize Spring Boot project
- [ ] Create model classes
- [ ] Implement Kafka producer config + EventPublisher
- [ ] Implement EventController (POST endpoints)
- [ ] Implement MongoDB repositories + AnalyticsService
- [ ] Implement AnalyticsController (GET endpoints)
- [ ] Configure CORS
- [ ] Write integration tests
- [ ] Add error handling + logging

## Success Criteria
- POST /api/events returns 202, message appears in Kafka topic (verify in Kafka UI)
- POST /api/events/batch handles 100 events in < 50ms
- GET endpoints return data from MongoDB with proper pagination
- Invalid events return 400 with validation errors
- CORS headers present for frontend origin

## Risk Assessment
- **Kafka unavailable:** async send will queue then fail. Add retry config (3 retries, 100ms backoff).
- **MongoDB slow queries:** add compound indexes on (userId, startTime) and (pageUrl, windowStart).
- **Memory under batch load:** limit batch size to 100 events per request.

## Security Considerations
- Validate all input fields (reject XSS in targetElement, URL injection in pageUrl)
- Rate limit /api/events: 1000 req/s per client IP (use Spring's RateLimiter or API gateway)
- No authentication for dev; add JWT validation for prod

## Next Steps
- Phase 4, 5, 6 consume from the Kafka topic this service produces to
- Phase 7 calls both POST (ingestion) and GET (analytics) endpoints
