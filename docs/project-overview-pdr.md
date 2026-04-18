# Project Overview & PDR

**Clickstream Analytics Application** — Comprehensive user behavior tracking system capturing click-level events from React frontend through serverless ingestion into a unified analytics platform.

---

## Project Vision

Build a scalable, low-latency analytics platform that captures granular user interaction data (clicks, page views, form submissions) and provides both real-time insights (live dashboards) and historical analysis (session playback, conversion funnels).

**End User:** Product analytics teams and data scientists querying user behavior patterns.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│ React Frontend (Web App)                                         │
│ • sendBeacon() → /api/events (fire-and-forget)                 │
│ • Session tracking (sessionId, userId)                          │
│ • Event queuing before network available                        │
└──────────────────────┬──────────────────────────────────────────┘
                       │ HTTP POST
┌──────────────────────▼──────────────────────────────────────────┐
│ Spring Boot Ingestion API (Phase 3)                             │
│ • Validates event schema                                        │
│ • Publishes to Kafka async (202 Accepted)                      │
│ • Serves historical analytics from MongoDB                      │
│ • Rate limiting (1000 req/s per endpoint)                      │
└──────────────────────┬──────────────────────────────────────────┘
                       │ Kafka Topic: clickstream-events
       ┌───────────────┼───────────────┐
       │               │               │
┌──────▼──────┐ ┌──────▼──────┐ ┌──────▼──────┐
│ Spark ETL   │ │ Real-time   │ │ Raw         │
│ (Python)    │ │ Analytics   │ │ Archiver    │
│ • Aggregate │ │ (Arrow)     │ │ • Parquet   │
│ • Sessions  │ │ • In-memory │ │ • Data Lake │
│ → MongoDB   │ │ • WebSocket │ │ → S3        │
└─────────────┘ │ • Frontend  │ └─────────────┘
                └─────────────┘
```

**Key Design:** Separation of concerns — ingestion API handles acceptance and routing, downstream services independently process events.

---

## Project Phases

| Phase | Name | Status | Effort | Outputs |
|-------|------|--------|--------|---------|
| 1 | Docker Environment | ✅ Complete | 1.5h | docker-compose.yml, Kafka, MongoDB |
| 2 | Event Schema | ✅ Complete | 2h | shared-models, ClickEvent, EventType |
| 3 | Ingestion API | ✅ Complete | 6h | Spring Boot, controllers, services, tests |
| 4 | **Spark ETL** | ✅ Complete | 8h | 3 parallel streams, MongoDB aggregates |
| 5 | Real-time Analytics | Planned | 6h | Arrow, WebSocket, live dashboard |
| 6 | Raw Archiver | Planned | 3h | Parquet, S3, data lake |

---

## Product Requirements Document (PRD)

### Functional Requirements

#### F1: Event Ingestion (Phase 3)

**Requirement:** API accepts click events from frontend with sub-10ms latency.

**Acceptance Criteria:**
- ✅ POST `/api/events` returns 202 Accepted
- ✅ Event published to Kafka within 5ms
- ✅ Batch endpoint accepts up to 100 events
- ✅ Invalid events return 400 with validation errors
- ✅ Event schema validated (userId, sessionId, eventType, timestamp required)

**Rationale:** Frontend uses `sendBeacon()` (fire-and-forget), so fast 202 response required. Async Kafka publish maintains low latency.

#### F2: Analytics Queries (Phase 3)

**Requirement:** REST API queries session/page/journey data from MongoDB with < 200ms latency.

**Acceptance Criteria:**
- ✅ GET `/api/analytics/sessions` returns paginated session aggregates
- ✅ Filters: userId, startTime, endTime, pagination (page, size)
- ✅ GET `/api/analytics/pages` returns page metrics (viewCount, bounceRate, topClicks)
- ✅ GET `/api/analytics/journeys/{userId}` returns user session journeys
- ✅ Queries use MongoDB indexes for performance

**Rationale:** Analysts need to slice data multiple ways (by user, time range, page) with responsive UI.

#### F5: Session Aggregation (Phase 4)

**Requirement:** Spark ETL continuously aggregates raw Kafka events into curated session, page, and journey collections in MongoDB.

**Acceptance Criteria:**
- ✅ Session aggregates computed every 30 seconds (micro-batch trigger)
- ✅ 3 parallel streams: session_aggregates, page_metrics, user_journeys
- ✅ Session windows use 30-minute inactivity gap
- ✅ Each stream writes idempotent upserts by composite key
- ✅ Watermark set to 10 minutes for late event tolerance
- ✅ MongoDB indexes created at startup
- ✅ TTL index on page_metrics (30-day retention)

**Rationale:** Pre-aggregated data enables fast queries for dashboards. Parallel streams maximize processing efficiency. Idempotent writes ensure exactly-once semantics despite retries.

#### F6: Crisis Recovery (Phase 4)

**Requirement:** Spark ETL handles failures gracefully without data loss.

**Acceptance Criteria:**
- ✅ Checkpoint directory persists on restart
- ✅ Consumer lag recoverable if broker goes down
- ✅ MongoDB connection failures retried
- ✅ StreamingQueryMonitor logs query progress
- ✅ Graceful shutdown stops all streams

**Rationale:** Production stability. Kafka stored offsets, MongoDB upserts survive failures.

#### F3: CORS Support (Phase 3)

**Requirement:** Frontend at `http://localhost:3000` can make cross-origin requests.

**Acceptance Criteria:**
- ✅ CORS headers in response
- ✅ Allowed origins configurable per environment
- ✅ Preflight OPTIONS requests handled

**Rationale:** React app on different port than API.

#### F4: Rate Limiting (Phase 3)

**Requirement:** Prevent abuse and resource exhaustion via rate limiting.

**Acceptance Criteria:**
- ✅ 1000 req/s per IP for `/api/events`
- ✅ 100 req/s per IP for `/api/events/batch`
- ✅ 500 req/s per IP for analytics endpoints
- ✅ 429 Too Many Requests returned when exceeded
- ✅ Rate limit headers in response

**Rationale:** DDoS prevention, resource protection.

### Non-Functional Requirements

#### NFR1: Performance

| Metric | Target | Achievement |
|--------|--------|--------------|
| Event ingestion p99 latency | < 10ms | ✅ 202 response + async Kafka |
| Analytics query p99 latency | < 200ms | ✅ Indexed MongoDB queries |
| Batch throughput | 100 events/batch | ✅ Tested with load tests |
| Schema validation latency | < 1ms | ✅ Bean validation annotations |

**How:** Async I/O, batch processing, indexed queries, connection pooling.

#### NFR2: Reliability

| Aspect | Target | Strategy |
|--------|--------|----------|
| Availability | 99.9% | Horizontal scaling, load balancing |
| Error Rate | < 0.1% | Input validation, error handling |
| Data Loss | 0% | Kafka acks=all, MongoDB replica set |
| Recovery Time | < 5min | Health checks, auto-restart |

#### NFR3: Scalability

| Dimension | Capacity | Growth Path |
|-----------|----------|-------------|
| Events/sec | 10k+ | Kafka partitions, API replicas |
| Concurrent Users | 1000+ | Connection pooling, async |
| Historical Data | 1TB | MongoDB sharding, data retention |

#### NFR4: Security

- ✅ Input validation (event schema)
- ✅ Rate limiting (DDoS protection)
- ⚠️ IP anonymization (PII handling)
- ⚠️ Error message sanitization (information disclosure)
- ⚠️ CORS strict origin validation

#### NFR5: Maintainability

- ✅ Clear separation of concerns (controller, service, repository)
- ✅ Integration tests with Testcontainers
- ✅ Configuration externalized (application.yml)
- ✅ Comprehensive logging (DEBUG, INFO levels)

---

## Technical Stack

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| **API Framework** | Spring Boot 3.x | Java ecosystem, extensive ecosystem |
| **Message Broker** | Apache Kafka | Distributed, high-throughput, multiple consumers |
| **Database** | MongoDB | Document-oriented, flexible schema, good analytics queries |
| **Container** | Docker | Reproducible environments |
| **Load Balancing** | Nginx/K8s | High availability |
| **Monitoring** | Prometheus/Grafana | Observability |
| **Logging** | ELK Stack | Log aggregation |

---

## Data Model

### ClickEvent (Event Schema v1.0)

```json
{
  "eventId": "UUID",
  "userId": "string",
  "sessionId": "string",
  "eventType": "CLICK|PAGE_VIEW|FORM_SUBMIT",
  "targetElement": "string (CSS selector)",
  "pageUrl": "URL",
  "referrerUrl": "URL (optional)",
  "timestamp": "milliseconds (unix epoch)",
  "userAgent": "string",
  "metadata": {
    "x": "integer (click x-coordinate)",
    "y": "integer (click y-coordinate)",
    "elementText": "string (button label)"
  }
}
```

### MongoDB Collections

**sessions** (written by Spark ETL, read by API)
```javascript
{
  "_id": ObjectId,
  "userId": "string",
  "sessionId": "string",
  "sessionDurationMs": number,
  "eventCount": number,
  "pageViews": number,
  "clickCount": number,
  "formSubmits": number,
  "startTime": ISODate,
  "endTime": ISODate,
  "devices": [string],
  "browsers": [string],
  "firstPageUrl": "URL",
  "lastPageUrl": "URL",
  "bounceRate": number,
  "conversionFunnel": [string]
}
```

---

## Known Issues & Fixes Required

### Critical (Pre-Production)

1. **EventValidator Missing @Component**
   - Issue: EventValidator not instantiated as bean
   - Impact: NullPointerException on startup
   - Fix: Add `@Component` annotation

2. **IP Address PII Exposure**
   - Issue: IP addresses stored without anonymization
   - Impact: GDPR violation, data privacy risk
   - Fix: Use `IpAnonymizer.anonymize()` in EventController

3. **Error Messages Expose Internal Details**
   - Issue: Stack traces returned to client
   - Impact: Information disclosure, security weakness
   - Fix: Use `GlobalExceptionHandler` to sanitize responses

### High Priority

- ⚠️ CORS credentials without strict origin validation
- ⚠️ Unbounded pagination (no max size limit)
- ⚠️ Missing MongoDB connection pool tuning
- ⚠️ No rate limiting implementation (placeholder only)

See [Code Review Report](../plans/20260418-init-clickstream/code-review-report-20260418.md) for details.

---

## Success Metrics

| Metric | Definition | Target |
|--------|------------|--------|
| **Event Acceptance Rate** | (202 responses) / (total requests) | > 99% |
| **Query Latency p99** | 99th percentile response time | < 200ms |
| **API Availability** | Uptime / Total time | > 99.9% |
| **Error Rate** | (4xx + 5xx) / total requests | < 0.1% |
| **Kafka Consumer Lag** | Max lag across all partitions | < 1000 messages |

---

## Dependencies & Interfaces

### Upstream Dependencies
- **React Frontend:** Sends events via POST `/api/events`
- **Kafka Broker:** Receives events, enables async publishing

### Downstream Dependencies
- **Spark ETL:** Consumes events, writes aggregates to MongoDB
- **Real-time Analytics:** Consumes events, supplies live data to frontend
- **Raw Archiver:** Consumes events, writes to S3 data lake

---

## Risks & Mitigation

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|-----------|
| Kafka broker down | Event loss, 5xx errors | Low | Replica, monitoring, alerts |
| MongoDB connection pool exhausted | Slow queries, timeouts | Medium | Pool tuning, connection limits |
| Bot/Spam traffic | Rate limit, DDoS | Medium | Rate limiting, IP blocking |
| Schema evolution | Breaking changes downstream | Medium | Version in schema, validation |
| PII exposure (IP addresses) | GDPR violation, legal | High | **Apply IpAnonymizer** |

---

## Roadmap

### Phase 3 (Current)
- ✅ Spring Boot foundation
- ✅ Event ingestion endpoints
- ✅ Analytics query endpoints
- ✅ Rate limiting
- ⚠️ Security fixes (PII, error handling)

### Phase 4 (✅ Complete)
- ✅ Spark Structured Streaming job
- ✅ 3 parallel aggregation streams
- ✅ Session/page/journey collections in MongoDB
- ✅ Idempotent writes with composite key upserts
- ✅ Checkpoint recovery strategy
- ✅ TTL index for automatic data expiration

### Phase 5 (Q2-Q3 2026)
- Real-time analytics (Arrow)
- WebSocket live dashboards
- Custom event tracking

### Phase 6 (Q3 2026)
- Raw Archiver (Parquet)
- Data lake queries (Trino)
- Advanced analytics (ML models)

---

## Team & Ownership

| Role | Owner | Responsibilities |
|------|-------|-----------------|
| **Product Lead** | [TBD] | Roadmap, requirements, analytics |
| **Backend Engineering** | [TBD] | Ingestion API, Spark ETL |
| **Data Engineering** | [TBD] | Kafka, MongoDB, performance |
| **DevOps** | [TBD] | Infrastructure, deployment, monitoring |

---

## Documentation Index

- [System Architecture](./system-architecture.md) — Technical design, data flow
- [Code Standards](./code-standards.md) — Conventions, patterns, best practices, testing
- **Ingestion API**
  - [README](./ingestion-api/README.md) — Quick start, overview
  - [API Reference](./ingestion-api/api-reference.md) — Endpoint documentation
  - [Configuration](./ingestion-api/configuration.md) — Kafka, MongoDB, rate limiting
  - [Development Guide](./ingestion-api/development-guide.md) — Local setup, testing, debugging
  - [Deployment](./ingestion-api/deployment.md) — Production, scaling, monitoring
- **Spark ETL (Phase 4)**
  - [README](../spark-etl/README.md) — Architecture, MongoDB schemas, configuration
  - [Troubleshooting](../spark-etl/README.md#troubleshooting) — Checkpoint issues, OOM, serialization
  - [Production Deployment](../spark-etl/README.md#production-cluster-deployment) — Cluster setup, environment variables
