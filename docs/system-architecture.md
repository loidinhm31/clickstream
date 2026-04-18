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
                    (Python)       Analytics       (Python)
                        │          (Arrow)              │
                    MongoDB         WebSocket         S3
                    Collections     Frontend       Data Lake
```

---

## Component Architecture

### Layer 1: Ingestion API (Phase 3 — Spring Boot)

**Responsibility:** Accept, validate, and route events to Kafka

```
┌────────────────────────────────────────────────────────┐
│ Spring Boot Application (port 8081)                    │
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
│ └─ raw-archiver: serialize → Parquet → S3          │
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

#### Phase 4: Spark ETL (Python)

```
Kafka Consumer (group: spark-etl)
         ↓ Read events
  Spark Streaming
         ↓ Aggregate
    • Group by userId, sessionId
    • Calculate session duration, page sequence
    • Extract metrics (bounce rate, conversion funnel)
         ↓ Write
    MongoDB (sessions, page_metrics, user_journeys)
         ↓
    Ready for API queries
```

#### Phase 5: Real-time Analytics (Arrow)

```
Kafka Consumer (group: realtime-analytics)
         ↓ Read events
    Apache Arrow (in-memory)
         ↓ Store
    Arrow in-memory tables per session
         ↓ Stream
    WebSocket → Frontend
         ↓
    Live dashboards update
```

#### Phase 6: Raw Archiver (Python)

```
Kafka Consumer (group: raw-archiver)
         ↓ Read events
    Convert to Parquet
         ↓ Partition
    By date: s3://bucket/clickstream/2026-04-18/
         ↓
    S3 Data Lake
         ↓
    Query via Trino/Athena
```

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

3. KAFKA TOPIC
   └─ clickstream-events partition (based on sessionId)
   └─ Event persisted, retention 24h

4. SPARK ETL (Consumer 1)
   └─ Read event from Kafka
   └─ Aggregate: {userId, sessionId, eventCount, pages visited, ...}
   └─ Write to MongoDB collections

5. REAL-TIME ANALYTICS (Consumer 2)
   └─ Read event from Kafka
   └─ Keep in-memory Arrow table
   └─ Push via WebSocket to frontend

6. RAW ARCHIVER (Consumer 3)
   └─ Read event from Kafka
   └─ Convert to Parquet
   └─ Write to S3 data lake

7. ANALYTICS QUERIES (API)
   └─ Frontend queries GET /api/analytics/sessions
   └─ API reads from MongoDB (aggregated by Spark)
   └─ Return paginated results
```

---

## Scalability Architecture

### Horizontal Scaling Strategy

```
Single Instance (Development)
┌─────────────┐
│ API Pod 1   │
│ Port 8081   │
└─────┬───────┘
      │ Round-robin
    Kafka

Multiple Instances (Production)
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ API Pod 1   │     │ API Pod 2   │     │ API Pod 3   │
│ Port 8081   │     │ Port 8081   │     │ Port 8081   │
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
    ├─ Kafka (internal port 9092)
    └─ MongoDB (internal port 27017)

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
