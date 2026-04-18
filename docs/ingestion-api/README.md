# Ingestion API - Phase 3

Spring Boot REST API for real-time clickstream event ingestion and historical analytics queries. Handles two distinct responsibilities:
1. **Ingestion** - Receive click events via HTTP POST → publish to Kafka asynchronously
2. **Analytics** - Serve historical session/page/journey data from MongoDB via REST queries

## Quick Start

### Prerequisites
- Java 17+ and Maven 3.9+
- Docker & Docker Compose (for MongoDB, Kafka)
- WSL 2 (Windows users)

### Running Locally

1. **Start infrastructure:**
```bash
cd ../..  # Go to clickstream root
docker compose up -d kafka mongo  # Starts Kafka (9092) and MongoDB (27017)
```

2. **Build and run API:**
```bash
cd ingestion-api
mvn clean install
mvn spring-boot:run
```

The API runs on `http://localhost:8081` with:
- Event ingestion: `POST /api/events`
- Analytics queries: `GET /api/analytics/*`

### Verify Installation

```bash
# Ingest a test event
curl -X POST http://localhost:8081/api/events \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "e1",
    "eventType": "CLICK",
    "userId": "user1",
    "sessionId": "sess1",
    "targetElement": "button",
    "pageUrl": "https://example.com",
    "timestamp": 1712678400000
  }'

# Should respond: 202 Accepted
```

## Architecture Overview

```
Frontend (React)
    ↓ sendBeacon() - fire-and-forget
EventController (/api/events)
    ↓ Fire-and-forget async
EventPublisher → Kafka (clickstream-events topic)
                     ↓ Partition key: sessionId
                
AnalyticsController (/api/analytics)
    ↓ Reads
MongoDB (clickstream_db)
    ├── SessionAggregate collection (written by Spark ETL)
    ├── PageMetric collection
    └── UserJourney collection
```

## Key Features

| Feature | Details |
|---------|---------|
| **Async Ingestion** | 202 Accepted response - event published to Kafka without blocking |
| **Batch Support** | POST `/api/events/batch` - up to 100 events per request |
| **Session Affinity** | Kafka partition key = sessionId, ensures event ordering per user |
| **Indexed Queries** | MongoDB queries on indexed fields (userId, pageUrl, timestamp) |
| **CORS Enabled** | Frontend at http://localhost:3000 can make requests |
| **Validation** | Event schema validation with 400 Bad Request error responses |
| **Rate Limiting** | Bucket4j-based rate limiting (configurable per endpoint) |

## Performance Targets

| Metric | Target | Status |
|--------|--------|--------|
| Event ingestion (p99 latency) | < 10ms | ✅ Async Kafka publish |
| REST query (p99 latency) | < 200ms | ✅ Indexed MongoDB queries |
| Batch throughput | 100 events/request | ✅ Tested |
| Concurrent connections | 100+ | ✅ Connection pooling configured |

## Project Files

```
ingestion-api/
├── pom.xml                          # Maven build, Spring/Kafka/MongoDB deps
├── src/main/resources/
│   ├── application.yml              # Kafka, MongoDB, CORS config
│   └── application-docker.yml       # Docker profile overrides
├── src/main/java/com/clickstream/
│   ├── ClickstreamApplication.java  # Main entry point
│   ├── config/
│   │   ├── KafkaProducerConfig.java    # KafkaTemplate bean
│   │   ├── CorsConfig.java             # CORS configuration
│   │   ├── MongoIndexConfig.java       # Index creation
│   │   ├── RateLimitFilter.java        # Request rate limiting
│   │   └── SharedModelConfig.java      # Shared model registration
│   ├── controller/
│   │   ├── EventController.java        # POST /api/events endpoints
│   │   └── AnalyticsController.java    # GET /api/analytics endpoints
│   ├── service/
│   │   ├── EventPublisher.java         # Kafka async publisher
│   │   └── AnalyticsService.java       # MongoDB queries
│   ├── model/
│   │   ├── SessionAggregate.java       # MongoDB document
│   │   ├── PageMetric.java             # MongoDB document
│   │   └── UserJourney.java            # MongoDB document
│   ├── repository/
│   │   ├── SessionAggregateRepository.java  # Spring Data interface
│   │   ├── PageMetricRepository.java        # Spring Data interface
│   │   └── UserJourneyRepository.java       # Spring Data interface
│   ├── exception/
│   │   └── GlobalExceptionHandler.java     # Exception advice
│   └── util/
│       └── IpAnonymizer.java          # PII anonymization utility
└── src/test/java/com/clickstream/
    └── controller/
        ├── EventControllerIntegrationTest.java
        └── AnalyticsControllerIntegrationTest.java
```

## Documentation

- **[API Reference](./api-reference.md)** - Full endpoint documentation with examples
- **[Configuration](./configuration.md)** - Kafka, MongoDB, rate limiting, CORS setup
- **[Development Guide](./development-guide.md)** - Testing, debugging, local setup
- **[Deployment](./deployment.md)** - Production configuration, indexes, pooling, monitoring

## Known Issues & Fixes

**Critical (Pre-Production):**
- ❌ EventValidator missing @Component annotation - Add to config
- ❌ IP addresses stored without anonymization - Apply IpAnonymizer utility
- ❌ Error responses expose internal details - Use GlobalExceptionHandler

See [Phase 03 Plan](../../plans/20260418-init-clickstream/phase-03-ingestion-api.md#code-review-findings) for full review.

## Related Components

- **[Shared Models](../shared-models/)** - ClickEvent, EventType schema
- **[Kafka](../docker-compose.yml)** - Message broker (localhost:9092)
- **[MongoDB](../docker-compose.yml)** - Document database (localhost:27017)
- **[Spark ETL](../spark-etl/)** - Writes session aggregates to MongoDB
- **[Real-time Analytics](../realtime-analytics/)** - WebSocket consumer for live data
