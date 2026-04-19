# Configuration Reference

Comprehensive configuration guide for Kafka, MongoDB, rate limiting, and CORS.

## Application Configuration (application.yml)

### Spring Boot Application Settings

```yaml
spring:
  application:
    name: clickstream-ingestion-api
```

Port assignment:
- **API Server:** 9051 (9050 reserved for Kafka UI)

---

## Kafka Configuration

### Producer Settings

**Location:** `application.yml` → `spring.kafka.*`

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9056
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: 1                    # Leader acknowledgment only (fast)
      compression-type: lz4      # LZ4 compression for network efficiency
      batch-size: 16384         # 16KB batch size
      linger-ms: 5              # Wait up to 5ms for batch accumulation
      retries: 3                # Retry failed sends 3 times
      retry-backoff-ms: 100     # Back off 100ms between retries
```

### Configuration Details

| Setting | Value | Reasoning |
|---------|-------|-----------|
| `bootstrap-servers` | `localhost:9056` | Single broker development; use `broker1:9056,broker2:9056,...` for cluster |
| `acks` | `1` | Leader ack only = low latency (< 10ms p99). For high durability use `all` |
| `compression-type` | `lz4` | LZ4 = fast compression (< 1ms). Alternative: `snappy`, `gzip`, `zstd` |
| `batch-size` | `16384` (16KB) | Balance between throughput and latency |
| `linger-ms` | `5` | Small window to preserve low latency (sendBeacon is fire-and-forget) |
| `retries` | `3` | Transient failures (network glitch) retry automatically |
| `retry-backoff-ms` | `100` | Exponential backoff prevents overwhelming broker |

### Topic Configuration

**Topic Name:** `clickstream-events`
- **Partitions:** 6 (supports ~6 concurrent sessions without contention)
- **Replication Factor:** 1 (development) → 3 (production)
- **Retention:** 24 hours (1 day)
- **Partition Key:** `sessionId` (all events from session → same partition → ordered)

#### To Create/Update Topic (via CLI):

```bash
# Create topic (Docker container)
docker exec kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9056 \
  --topic clickstream-events \
  --partitions 6 \
  --replication-factor 1 \
  --retention-ms 86400000  # 24 hours

# Verify
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9056

# Describe
docker exec kafka kafka-topics.sh --describe \
  --bootstrap-server localhost:9056 \
  --topic clickstream-events
```

---

## MongoDB Configuration

### Connection Settings

```yaml
spring:
  data:
    mongodb:
      uri: mongodb://localhost:9055/clickstream_db
      auto-index-creation: false  # Disable auto-index in production

spring.data.mongodb:
  max-pool-size: 100            # Connection pool maximum
  min-pool-size: 10             # Minimum idle connections
  max-connection-idle-time: 60000  # Close idle connections after 60s
  max-connection-life-time: 1800000  # Max connection lifetime 30min
```

### Configuration Details

| Setting | Value | Purpose |
|---------|-------|---------|
| `uri` | `mongodb://localhost:9055/clickstream_db` | Connection string (username/password for production) |
| `auto-index-creation` | `false` | Manually create indexes via MongoIndexConfig (recommended for production) |
| `max-pool-size` | `100` | Max concurrent connections |
| `min-pool-size` | `10` | Maintain minimum idle pool |
| `max-connection-idle-time` | `60000` ms | Close idle after 60s |
| `max-connection-life-time` | `1800000` ms | Force new connection after 30min |

### Collections & Indexes

**Collections (written by Spark ETL, read by API):**

1. **sessions**
   - Indexed on: `userId`, `startTime`, `sessionId`
   - Used by: `GET /api/analytics/sessions`

2. **page_metrics**
   - Indexed on: `pageUrl`, `lastUpdated`
   - Used by: `GET /api/analytics/pages`

3. **user_journeys**
   - Indexed on: `userId`, `sessionId`
   - Used by: `GET /api/analytics/journeys/{userId}`

#### To Create Indexes Manually:

```javascript
// Run in MongoDB shell
use clickstream_db

// Sessions collection
db.sessions.createIndex({ userId: 1, startTime: -1 })
db.sessions.createIndex({ sessionId: 1 })

// Page metrics
db.page_metrics.createIndex({ pageUrl: 1 })
db.page_metrics.createIndex({ lastUpdated: -1 })

// User journeys
db.user_journeys.createIndex({ userId: 1 })
db.user_journeys.createIndex({ sessionId: 1 })
```

**Index Creation in Code:** See `MongoIndexConfig.java` - automatic initialization on startup (development only).

---

## Rate Limiting Configuration

### Bucket4j Rate Limiter

**Location:** `RateLimitFilter.java`

```java
// Default limits per endpoint
POST /api/events       → 1000 requests/sec per IP
POST /api/events/batch → 100 requests/sec per IP
GET /api/analytics/*   → 500 requests/sec per IP
```

### Configuration Override

To modify rate limits, edit `RateLimitFilter.java`:

```java
private static final Bandwidth EVENT_LIMIT = 
    Bandwidth.classic(1000, Refill.intervally(1000, Duration.ofSeconds(1)));
// Change 1000 to desired limit (requests per second)
```

### Rate Limit Response

**When exceeded:**
```http
HTTP/1.1 429 Too Many Requests

{
  "status": 429,
  "message": "Rate limit exceeded",
  "retryAfter": 1
}
```

**Headers:**
- `X-Rate-Limit-Limit: 1000` - Limit for this endpoint
- `X-Rate-Limit-Remaining: 0` - Requests remaining in window
- `X-Rate-Limit-Reset: 1712678401` - Unix timestamp when limit resets
- `Retry-After: 1` - Seconds to wait before retrying

---

## CORS Configuration

### Cross-Origin Resource Sharing

```yaml
clickstream:
  cors:
    allowed-origins: http://localhost:3000
```

**Allowed Origins:** Configured per controller via `@CrossOrigin` annotation:

```java
@CrossOrigin(origins = "${clickstream.cors.allowed-origins:http://localhost:3000}")
```

**Default:** `http://localhost:3000` (React frontend)

### CORS Headers Sent

```
Access-Control-Allow-Origin: http://localhost:3000
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type, Authorization
Access-Control-Max-Age: 3600
```

### To Update Allowed Origins

**Option 1: Environment Variable**
```bash
export CLICKSTREAM_CORS_ALLOWED_ORIGINS="https://app.example.com,https://staging.example.com"
mvn spring-boot:run
```

**Option 2: application.yml**
```yaml
clickstream:
  cors:
    allowed-origins: "https://app.example.com,https://staging.example.com"
```

**Option 3: application-prod.yml** (production profile)
```yaml
spring:
  profiles:
    active: prod

clickstream:
  cors:
    allowed-origins: "https://analytics.example.com"
```

---

## Batch Processing Configuration

```yaml
clickstream:
  batch:
    max-size: 100  # Maximum events in POST /api/events/batch
```

**Validation:** Batch requests with > 100 events return 400 Bad Request.

---

## Logging Configuration

```yaml
logging:
  level:
    root: INFO
    com.clickstream: DEBUG
    org.springframework.kafka: INFO
    org.springframework.data.mongodb: INFO
  
  pattern:
    console: "%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"
```

### Log Levels

| Package | Level | Purpose |
|---------|-------|---------|
| `root` | INFO | Standard Spring Boot logs |
| `com.clickstream` | DEBUG | Application events (ingest, queries) |
| `org.springframework.kafka` | INFO | Kafka producer metrics |
| `org.springframework.data.mongodb` | INFO | MongoDB connection pool |

### Example Logs

```
2026-04-18 10:30:00.123 [main] INFO  com.clickstream.ClickstreamApplication - Starting Clickstream Application
2026-04-18 10:30:05.456 [http-nio-9051-exec-1] DEBUG com.clickstream.controller.EventController - Received single event: type=CLICK, sessionId=sess-xyz-789
2026-04-18 10:30:05.460 [kafka-producer-thread] DEBUG com.clickstream.service.EventPublisher - Publishing event: sessionId=sess-xyz-789, type=CLICK, eventId=e1
```

---

## Environment-Specific Profiles

### Development (default)

**File:** `application.yml`
- Kafka: localhost:9056
- MongoDB: localhost with auto-index creation enabled
- Logging: DEBUG
- CORS: http://localhost:3000

### Docker

**File:** `application-docker.yml`
- Kafka: kafka:9056 (service name)
- MongoDB: mongo:9055 (service name)
- Logging: INFO

**To use:**
```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--spring.profiles.active=docker"
```

### Production

**Create:** `application-prod.yml`

```yaml
spring:
  kafka:
    bootstrap-servers: kafka-broker-1:9056,kafka-broker-2:9056,kafka-broker-3:9056
    producer:
      acks: all  # Wait for all replicas
      retries: 10
  data:
    mongodb:
      uri: mongodb+srv://prod-user:${DB_PASSWORD}@cluster.mongodb.net/clickstream_db
      auto-index-creation: false
      ssl: true

clickstream:
  cors:
    allowed-origins: "https://analytics.example.com"
  kafka:
    topic: clickstream-events-prod
```

**To use:**
```bash
export SPRING_PROFILES_ACTIVE=prod
export DB_PASSWORD=your-password
java -jar ingestion-api-1.0.0.jar
```

---

## Performance Tuning

### For High Throughput (> 10k events/sec)

```yaml
spring:
  kafka:
    producer:
      batch-size: 65536        # Increase to 64KB
      linger-ms: 50            # Allow 50ms batching
      compression-type: snappy # Snappy = better compression ratio

spring.data.mongodb:
  max-pool-size: 200           # Increase connection pool
```

### For Low Latency (< 5ms p99)

```yaml
spring:
  kafka:
    producer:
      batch-size: 8192         # Decrease to 8KB
      linger-ms: 1             # Minimal batching
      acks: 1                  # Leader ack enough

server:
  tomcat:
    threads:
      max: 200                 # More HTTP threads
```

---

## Configuration Validation

On startup, the application validates:
- ✅ Kafka broker connectivity
- ✅ MongoDB connection and credentials
- ✅ Index creation (development only)
- ✅ CORS configuration is valid URL

**Startup Output:**
```
INFO: Kafka broker connected: 1 broker(s) available
INFO: MongoDB connected to clickstream_db
INFO: Indexes verified on collections: sessions, page_metrics, user_journeys
```

Startup fails if any configuration is invalid (see logs for details).
