# Clickstream Analytics Application

End-to-end clickstream analytics system capturing user micro-events from a React frontend, ingesting via Spring Boot into Kafka, then processing through three independent consumer groups: Spark ETL (→ MongoDB), Real-time Analytics (Arrow in-memory → Arrow Flight → frontend), and Raw Archiver (→ Parquet data lake).

## Architecture

```
React Frontend (micro-events)
    ↓
Spring Boot Ingestion API
    ↓
Apache Kafka (clickstream-events topic)
    ↓
    ├─→ Spark ETL → MongoDB (aggregated sessions)
    ├─→ Real-time Analytics (Arrow in-memory) → WebSocket → Frontend
    └─→ Raw Archiver → Parquet Data Lake
```

## Development Environment

### Prerequisites

- Docker & Docker Compose (in WSL for Windows users)
- Java 17+ (for Spring Boot)
- Node.js 18+ (for React frontend)
- Maven or Gradle

### Quick Start

1. **Start infrastructure services:**

```bash
# Start Kafka, MongoDB, and Kafka UI
docker compose up -d

# Verify setup (runs automated tests)
bash scripts/verify-setup.sh
```

2. **Access services:**

- **Kafka UI:** http://localhost:8080
- **Kafka Broker:** localhost:9092
- **MongoDB:** mongodb://localhost:27017/clickstream_db

### Services

| Service | Port | Description |
|---------|------|-------------|
| Apache Kafka | 9092 | Message broker (KRaft mode) |
| Kafbat UI | 8080 | Web UI for Kafka inspection |
| MongoDB | 27017 | Document database for aggregated data |

### Kafka Topics

- **clickstream-events** (6 partitions)
  - Partition key: `sessionId`
  - Retention: 24 hours (1 day)
  - Consumer groups: spark-etl, realtime-analytics, raw-archiver

## Event Schema (Phase 2)

### JSON Structure

Each event published to `clickstream-events` follows this schema (v1.0):

```json
{
  "schemaVersion": "1.0",
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "user-abc-123",
  "sessionId": "sess-xyz-789",
  "eventType": "CLICK",
  "targetElement": "button#submit-order",
  "pageUrl": "https://app.example.com/checkout",
  "referrerUrl": "https://app.example.com/cart",
  "timestamp": 1712678400000,
  "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
  "metadata": {
    "x": 450,
    "y": 320,
    "viewportWidth": 1920,
    "viewportHeight": 1080,
    "elementText": "Place Order"
  }
}
```

**Schema Version Tracking:** Versioned for future compatibility. Currently at `1.0`.

### Supported Event Types

| Type | Triggers | Key Metadata | Example Use Case |
|------|----------|--------------|------------------|
| **CLICK** | User clicks interactive element | x, y, targetElement, elementText | Track button clicks, link engagement |
| **PAGE_VIEW** | Page load or SPA route change | pageUrl, referrerUrl | Funnel analysis, conversion tracking |
| **SCROLL** | Scrolling past depth threshold (25%, 50%, 75%, 100%) | scrollDepth, viewportWidth, viewportHeight | Content engagement, page section interest |
| **HOVER** | Mouse hover on element > 500ms | targetElement, durationMs | Feature discovery, UX patterns |

**Note:** eventType determines which metadata fields are populated; others are null and omitted.

### Java Model Usage with Builder Pattern

All models are immutable with private constructors. Use builders for construction:

```java
// Create a CLICK event
ClickEvent clickEvent = ClickEvent.builder()
    .eventId(UUID.randomUUID().toString())
    .userId("user-abc-123")
    .sessionId(sessionId)
    .eventType(EventType.CLICK)
    .targetElement("button#submit-order")
    .pageUrl("https://app.example.com/checkout")
    .referrerUrl("https://app.example.com/cart")
    .timestamp(System.currentTimeMillis())
    .userAgent(request.getHeader("User-Agent"))
    .metadata(EventMetadata.builder()
        .x(450)
        .y(320)
        .elementText("Place Order")
        .viewportWidth(1920)
        .viewportHeight(1080)
        .build())
    .build();

// Validate before publishing
EventValidator validator = new EventValidator();
List<String> errors = validator.validate(clickEvent);
if (!errors.isEmpty()) {
    errors.forEach(System.err::println);
    // Handle validation errors
}

// Serialize to JSON (Jackson)
String json = objectMapper.writeValueAsString(clickEvent);
```

### Metadata Field Reference

All metadata fields are optional and type-specific:

| Field | Type | Range | Event Types | Description |
|-------|------|-------|-------------|-------------|
| `x` | Integer | 0-∞ | CLICK | Mouse X coordinate (pixels) |
| `y` | Integer | 0-∞ | CLICK | Mouse Y coordinate (pixels) |
| `scrollDepth` | Double | 0.0-1.0 | SCROLL | Scroll depth (0.25, 0.5, 0.75, 1.0) |
| `viewportWidth` | Integer | 320-∞ | CLICK, SCROLL, HOVER | Viewport width (pixels) |
| `viewportHeight` | Integer | 480-∞ | CLICK, SCROLL, HOVER | Viewport height (pixels) |
| `elementText` | String | 0-1024 chars | CLICK | Text content of clicked element |
| `durationMs` | Long | ≥500 | HOVER | Hover duration (milliseconds) |

## Validation Rules & Security

The `EventValidator` class enforces multiple layers of security and data quality checks:

### Security Validations

- **XSS Prevention:** Blocks scripts, onerror/onclick handlers, iframes in all string fields
  ```
  Blocked patterns: <script, javascript:, onerror=, onclick=, <iframe
  ```

- **PII Detection:** Prevents accidental collection of sensitive data
  ```
  Blocked keywords: password, ssn, social security, credit card, cvv, pin
  ```

- **IP Address Rejection:** Filters out IP addresses (privacy compliance)
  ```
  Blocked pattern: X.X.X.X format detection
  ```

- **Field Length Limits:** Prevents oversized messages and buffer issues
  ```
  eventId/userId/sessionId: max 255 chars
  pageUrl/referrerUrl: max 2048 chars
  targetElement: max 512 chars
  elementText: max 1024 chars
  userAgent: max 8192 chars
  ```

### Data Quality Validations

- **Timestamp Validation:**
  - Events must be ≤ 24 hours old
  - Events allowed up to 5 minutes in future (clock skew tolerance)

- **URL Validation:**
  - Must be valid HTTP(S) URLs
  - Validated using URI parsing

- **Event Type Consistency:**
  - CLICK events must have targetElement and x/y coordinates
  - PAGE_VIEW events must have pageUrl
  - SCROLL events must have scrollDepth between 0.0-1.0
  - HOVER events must have targetElement and durationMs ≥ 500ms

### Usage Example

```java
EventValidator validator = new EventValidator(
    86400000L,   // Max event age: 24 hours
    300000L      // Max future drift: 5 minutes
);

List<String> errors = validator.validate(event);
if (errors.isEmpty()) {
    kafkaTemplate.send("clickstream-events", event.getSessionId(), json);
} else {
    logger.warn("Validation failed: {}", String.join(", ", errors));
}
```

## Kafka Partitioning Strategy

### Session-Based Partitioning

The `clickstream-events` topic uses **sessionId as the partition key** (not userId):

**Why sessionId over userId?**
- **Load Distribution:** SessionId naturally avoids hot partitions. High-activity users don't create bottlenecks.
- **Event Ordering:** All events from one session → same partition → chronological ordering preserved
- **Efficient Processing:** Spark session windowing works within a single partition; no expensive cross-partition joins
- **Time Bounded:** Sessions expire (~30-60 min), ensuring even distribution over time

**Alternative rejected (userId):**
- Creates hot partitions: power users generate 10-100x more events than average
- Would require complex balancing logic

**Partition Configuration (6 partitions):**
```
Topic: clickstream-events
Replication Factor: 1 (single broker in dev)
Partitions: 6 (allows parallelism across 3-6 consumer threads)
Retention: 24 hours (1 day)
Max Message Size: 1 MB (field length limits enforce this)
Compression: snappy (default)
```

## Project Structure

```
clickstream/
├── docker-compose.yml          # Infrastructure setup
├── scripts/
│   └── verify-setup.sh         # Automated verification
├── plans/                      # Implementation plans
│   └── 20260418-init-clickstream/
│       ├── plan.md             # Master plan
│       └── phase-*.md          # Detailed phase plans
└── [TBD: service directories]
    ├── ingestion-api/          # Spring Boot REST API
    ├── spark-etl/              # Spark Structured Streaming
    ├── realtime-analytics/     # Arrow in-memory analytics
    ├── raw-archiver/           # Parquet writer
    └── frontend/               # React application
```

## Development Workflow

1. **Phase 01:** Dev environment (Docker Compose) ✓
2. **Phase 02:** Kafka topic design & event schema ✓
3. **Phase 03:** Spring Boot ingestion API
4. **Phase 04:** Spark ETL pipeline
5. **Phase 05:** Real-time analytics service (Arrow)
6. **Phase 06:** Raw event archiver
7. **Phase 07:** React frontend (Atomic Design)

See [plans/20260418-init-clickstream/plan.md](plans/20260418-init-clickstream/plan.md) for complete roadmap.

## Key Technologies

- **Apache Kafka** (KRaft mode) - Event streaming platform
- **Spring Boot** - REST API framework
- **Apache Spark** - Distributed data processing
- **Apache Arrow** - In-memory columnar format
- **MongoDB** - Document database
- **React** - Frontend framework
- **Parquet** - Columnar storage format

## Development Environment Details

### Why KRaft Mode (No ZooKeeper)

We use Apache Kafka in KRaft (Kraft Consensus) mode instead of the traditional ZooKeeper-based architecture for these reasons:

- **Simplified Operations:** Single control plane removes ZooKeeper coordination complexity
- **Reduced Resource Overhead:** No separate ZooKeeper cluster needed in development
- **Future-Ready:** KRaft is Kafka's recommended mode for versions 4.0+
- **Faster Boot:** Fewer services to initialize during startup
- **Operational Consistency:** Same metadata architecture as production deployments

In `docker-compose.yml`, note the environment variables:
- `KAFKA_PROCESS_ROLES: broker,controller` - Single node acts as both broker and controller
- `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093` - Single voter quorum for dev

### Resource Limits for Development

The Docker Compose services are configured with resource limits for safe local development:

| Service | Memory | CPU | Rationale |
|---------|--------|-----|-----------|
| Kafka | 2 GB | 2 CPU | Sufficient for message ingestion and buffering |
| MongoDB | 1 GB | 1 CPU | Document storage and indexing |
| Kafka UI | 1 GB | 1 CPU | Web interface for cluster inspection |
| kafka-init | 512 MB | 0.5 CPU | One-time topic initialization task |

These limits prevent runaway containers from consuming all system resources. For higher-throughput testing, adjust `mem_limit` and `cpus` in `docker-compose.yml`.

### Health Check Configuration

Each primary service includes health checks to ensure readiness before dependent services start:

- **Kafka:** Validates broker API versions (10s interval, 30s startup grace)
- **MongoDB:** Runs `db.adminCommand('ping')` (10s interval, 20s startup grace)
- **Kafka UI:** HTTP health endpoint check (10s interval, 20s startup grace)

The `kafka-init` service depends on Kafka's `service_healthy` condition, ensuring the broker is ready before creating topics.

## Testing

```bash
# Produce test event
echo '{"eventId":"test-1","eventType":"CLICK","timestamp":1234567890}' | \
  docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic clickstream-events

# Consume events
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic clickstream-events \
  --from-beginning
```

## Manage Services

```bash
# Start all services
docker compose up -d

# View logs
docker compose logs -f

# Stop all services
docker compose down

# Stop and remove volumes (clean slate / reset environment)
docker compose down -v
```

## Troubleshooting

### Port Conflicts

If you encounter "port already in use" errors:

```bash
# Check which process is using the port
# On Windows/WSL:
netstat -ano | findstr :9092
netstat -ano | findstr :8080
netstat -ano | findstr :27017

# On Linux/macOS:
lsof -i :9092
lsof -i :8080
lsof -i :27017

# Option 1: Stop the conflicting service
# Option 2: Change ports in docker-compose.yml
# Example: "9093:9092" instead of "9092:9092"
```

### Kafka Not Ready

If services fail to start or topics aren't created:

```bash
# Check Kafka logs
docker logs kafka

# Verify Kafka is responding
docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Restart with clean state
docker compose down -v
docker compose up -d
```

### MongoDB Connection Refused

If MongoDB connection fails:

```bash
# Check MongoDB logs
docker logs mongodb

# Test connection manually
docker exec mongodb mongosh --eval "db.version()" clickstream_db

# Verify port is listening
docker ps | grep mongodb
```

### WSL Docker Bridge Networking

If services can't communicate or localhost:9092 doesn't work from Windows:

```bash
# Verify Docker is running in WSL2 (not WSL1)
wsl -l -v

# Check Docker daemon is running
wsl docker ps

# Access services from WSL using localhost
# Access services from Windows using localhost (Docker Desktop required)

# Alternative: Use host.docker.internal instead of localhost in connection strings
```

### Slow Startup / Timing Issues

If kafka-init fails or verification times out:

```bash
# Increase wait time in verify-setup.sh 
# Health checks will ensure services are ready

# Check container health status
docker compose ps

# Wait for all services to be healthy
until docker inspect --format='{{.State.Health.Status}}' kafka | grep -q healthy; do sleep 2; done
```

### Images Not Pulling

If Docker can't pull images:

```bash
# Pre-pull images
docker compose pull

# Check Docker Hub connectivity
docker pull apache/kafka:3.7.0

# Use mirror if Docker Hub is blocked
# (edit docker-compose.yml with alternative registry)
```

## Next Steps

- [ ] Define event schema (Phase 02)
- [ ] Implement Spring Boot ingestion API (Phase 03)
- [ ] Build Spark ETL pipeline (Phase 04)
- [ ] Develop real-time analytics service (Phase 05)
- [ ] Create raw event archiver (Phase 06)
- [ ] Build React frontend (Phase 07)

## License

[TBD]

## Contributing

[TBD]
