# Development Guide

Local development setup, testing, debugging, and troubleshooting for the Ingestion API.

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| **Java** | 17+ | Spring Boot 3.x requirement |
| **Maven** | 3.9+ | Build tool |
| **Docker** | 20.10+ | Container runtime (Kafka, MongoDB) |
| **Docker Compose** | 1.29+ | Orchestration (docker-compose.yml) |
| **WSL 2** | Latest | Windows users only |
| **Git** | 2.30+ | Version control |
| **IDE** | IntelliJ IDEA / VS Code | Code editor (optional) |

## Setup

### 1. Clone Repository

```bash
cd ~/workspace
git clone https://github.com/yourorg/clickstream.git
cd clickstream
```

### 2. Start Infrastructure (Docker)

```bash
# Start Kafka broker and MongoDB
docker compose up -d kafka mongo

# Verify services are running
docker compose ps

# Expected output:
# NAME                PORTS
# kafka               0.0.0.0:9092->9092/tcp
# mongo               0.0.0.0:27017->27017/tcp
```

**Access:** 
- Kafka UI: http://localhost:8080 (check topics, messages)
- MongoDB: `mongosh localhost:27017` (CLI access)

### 3. Build Ingestion API

```bash
cd ingestion-api

# Full build (compile + test)
mvn clean install

# Quick build (skip tests)
mvn clean package -DskipTests

# Build output
# [INFO] BUILD SUCCESS
# ingestion-api-1.0.0-SNAPSHOT.jar
```

### 4. Run Application

**Option A: Maven Spring Boot Plugin (recommended)**
```bash
mvn spring-boot:run
```

**Option B: Java CLI**
```bash
# Build first
mvn clean package

# Run JAR
java -jar target/ingestion-api-1.0.0-SNAPSHOT.jar
```

**Expected Output:**
```
  .   ____          _            __ _ _
 /\\ / ___'_ __ _ _(_)_ __  __ _ \ \ \ \
( ( )\___ | '_ | '_| | '_ \/ _` | \ \ \ \
 \\/  ___)| |_)| | | | | || (_| |  ) ) ) )
  '  |____| .__|_| |_|_| |_\__, | / / / /
 =========|_|==============|___/=/_/_/_/
 
2026-04-18 10:30:00.123 INFO Starting ClickstreamApplication
...
2026-04-18 10:30:05.456 INFO Started ClickstreamApplication in 5.333 seconds (JVM running for 5.789)

Tomcat started on port 8081 with context path ''
```

Application is ready on `http://localhost:8081`

---

## Testing

### Unit Tests

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=EventControllerIntegrationTest

# Run with coverage
mvn test jacoco:report
# Open: target/site/jacoco/index.html
```

### Integration Tests

**Requires:** Docker running with Kafka & MongoDB

```bash
# Run integration tests (Testcontainers)
mvn verify

# Run specific integration test
mvn verify -Dtest=EventControllerIntegrationTest
```

**Test Profile:** Uses `application-test.yml` with embedded Kafka and MongoDB via Testcontainers.

### Manual Testing

#### Test 1: Single Event Ingestion

```bash
curl -X POST http://localhost:8081/api/events \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "e1",
    "userId": "user1",
    "sessionId": "sess1",
    "eventType": "CLICK",
    "targetElement": "button#submit",
    "pageUrl": "https://app.example.com",
    "timestamp": '$(date +%s000)'
  }'

# Expected response: 202 Accepted
# {"success": true, "message": "Event accepted", "errors": null}
```

#### Test 2: Batch Ingestion

```bash
curl -X POST http://localhost:8081/api/events/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"eventId": "e1", "userId": "u1", "sessionId": "s1", "eventType": "PAGE_VIEW", "pageUrl": "https://example.com", "timestamp": '$(date +%s000)'},
    {"eventId": "e2", "userId": "u1", "sessionId": "s1", "eventType": "CLICK", "pageUrl": "https://example.com", "timestamp": '$(date +%s000)'}
  ]'

# Expected response: 202 Accepted
```

#### Test 3: Verify Kafka Topic

```bash
# Check if events appear in Kafka topic
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic clickstream-events \
  --from-beginning \
  --max-messages 5

# Expected output: JSON events from ingestion
```

#### Test 4: Query Analytics

```bash
curl http://localhost:8081/api/analytics/sessions?page=0&size=10

# Expected response:
# {
#   "content": [...],
#   "pageable": {"pageNumber": 0, "pageSize": 10, ...}
# }

# Note: No data until Spark ETL processes events (separate service)
```

#### Test 5: Batch Size Limit

```bash
# Generate 101 events
python3 << 'EOF'
import json
events = [
  {
    "eventId": f"e{i}",
    "userId": "user1",
    "sessionId": "sess1",
    "eventType": "CLICK",
    "pageUrl": "https://example.com",
    "timestamp": 1712678400000 + i*1000
  }
  for i in range(101)
]
print(json.dumps(events))
EOF | curl -X POST http://localhost:8081/api/events/batch \
  -H "Content-Type: application/json" \
  -d @-

# Expected response: 400 Bad Request
# {"success": false, "message": "Batch validation failed", "errors": ["Batch size 101 exceeds maximum 100"]}
```

#### Test 6: Validation Errors

```bash
# Missing required field
curl -X POST http://localhost:8081/api/events \
  -H "Content-Type: application/json" \
  -d '{"eventType": "CLICK"}'  # Missing userId, sessionId, etc.

# Expected response: 400 Bad Request
# {"success": false, "message": "Validation failed", "errors": ["userId must not be blank", "sessionId must not be blank", ...]}
```

#### Test 7: CORS Preflight

```bash
curl -X OPTIONS http://localhost:8081/api/events \
  -H "Origin: http://localhost:3000" \
  -H "Access-Control-Request-Method: POST"

# Expected response: 200 OK
# Response headers include:
# Access-Control-Allow-Origin: http://localhost:3000
# Access-Control-Allow-Methods: GET, POST, OPTIONS
```

---

## Debugging

### IDE Setup (IntelliJ IDEA)

1. **Open Project:**
   - File → Open → Select `clickstream` directory
   - Maven will auto-detect `pom.xml` and download dependencies

2. **Run with Debugger:**
   - Right-click `ClickstreamApplication.java` → Run with Debugger
   - Or: Click "Debug" button in gutter

3. **Set Breakpoints:**
   - Click line number in `EventController.java`
   - Debug session will pause at breakpoint

4. **Inspect Variables:**
   - Watch for `event`, `errors`, `sessionId` values
   - Evaluate expressions in console

### Console Logging

**Application logs appear in terminal:**

```
2026-04-18 10:30:05.456 [http-nio-8081-exec-1] DEBUG com.clickstream.controller.EventController - Received single event: type=CLICK, sessionId=sess-xyz-789
2026-04-18 10:30:05.460 [kafka-producer-thread] DEBUG com.clickstream.service.EventPublisher - Publishing event: sessionId=sess-xyz-789, type=CLICK
2026-04-18 10:30:05.465 [http-nio-8081-exec-1] INFO  com.clickstream.controller.EventController - Event accepted: eventId=e1
```

**Increase Verbosity:**
```bash
# Run with trace logging
mvn spring-boot:run -Dspring-boot.run.arguments="--logging.level.com.clickstream=TRACE"
```

### Health Check

```bash
curl http://localhost:8081/actuator/health

# Expected response:
# {"status": "UP"}
```

### Kafka Diagnostics

**Check topics exist:**
```bash
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
# Should show: clickstream-events
```

**Monitor topic throughput:**
```bash
docker exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group kafka-consumer-group \
  --describe
```

**Reset topic (clear all messages):**
```bash
docker exec kafka kafka-topics.sh --delete \
  --bootstrap-server localhost:9092 \
  --topic clickstream-events

# Recreate topic
docker exec kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic clickstream-events \
  --partitions 6
```

### MongoDB Diagnostics

**Connect via CLI:**
```bash
mongosh localhost:27017
use clickstream_db

# Check collections exist
show collections
# Expected: sessions, page_metrics, user_journeys

# Check documents
db.sessions.countDocuments()
db.sessions.findOne()  # View one document
```

**View indexes:**
```bash
db.sessions.getIndexes()
```

---

## Troubleshooting

### Issue: "Kafka broker not available"

**Symptom:** 
```
ERROR: java.net.ConnectException: Connection refused (Connection refused)
```

**Solution:**
```bash
# Check if Kafka is running
docker compose ps

# If not running, start it
docker compose up -d kafka mongo

# Check logs
docker logs kafka
docker logs mongo
```

### Issue: "MongoDB connection timeout"

**Symptom:**
```
ERROR: com.mongodb.MongoTimeoutException: Timed out after 10000 ms
```

**Solution:**
```bash
# Check MongoDB is running
docker compose ps mongo

# Verify connection
mongosh localhost:27017

# If stuck, restart container
docker compose restart mongo
```

### Issue: "Rate limit exceeded on first request"

**Symptom:**
```
HTTP 429 Too Many Requests
```

**Solution:**
1. Check if filter is misconfigured
2. Increase `Bandwidth` in `RateLimitFilter.java`
3. Restart application: `Ctrl+C` and `mvn spring-boot:run`

### Issue: "Port 8081 already in use"

**Symptom:**
```
ERROR: Address already in use: bind
```

**Solution:**
```bash
# Find process using port 8081
lsof -i :8081

# Kill it
kill -9 <PID>

# Or use different port
mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=8082"
```

### Issue: "No events appear in Kafka topic"

**Symptom:** POST /api/events returns 202, but no messages in topic

**Root Causes:**
1. Event validation fails silently
2. Kafka partition key is null (sessionId empty)
3. Kafka send fails asynchronously

**Debug:**
```bash
# Add logging in EventPublisher
logger.info("Publishing to topic={}, partition_key={}, value_length={}", 
    topic, key, value.length());

# Check AsyncKafkaTemplate callbacks
future.whenComplete((result, ex) -> {
    if (ex != null) logger.error("Kafka send failed", ex);
});

# Restart and send event
mvn spring-boot:run

# Monitor Kafka consumer
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic clickstream-events \
  --from-beginning
```

### Issue: "CORS error in browser"

**Symptom:**
```
Access to XMLHttpRequest at 'http://localhost:8081/api/events' 
from origin 'http://localhost:3000' has been blocked by CORS policy
```

**Solution:**
```yaml
# Check application.yml
clickstream:
  cors:
    allowed-origins: http://localhost:3000

# OR set env var
export CLICKSTREAM_CORS_ALLOWED_ORIGINS="http://localhost:3000"

# Restart service
mvn spring-boot:run
```

---

## Development Workflow

### Making Changes

1. **Edit Java file** (e.g., `EventController.java`)
2. **Hot reload:** Spring Dev Tools auto-restarts on file change (if using `spring-boot-devtools`)
3. or **Manual restart:** `Ctrl+C` and `mvn spring-boot:run`
4. **Test:** `curl` or Postman
5. **Check logs** for errors

### Committing Code

```bash
# Run tests before commit
mvn clean test

# Check code quality
mvn spotbugs:check  # If plugin enabled

# Commit
git add .
git commit -m "feat: add IpAnonymizer utility"

# Push
git push origin feature/my-feature
```

---

## Performance Testing

### Load Testing with Apache Bench

```bash
# Test single event endpoint
ab -n 1000 -c 100 -p event.json -T application/json \
   http://localhost:8081/api/events

# Output shows:
# Requests per second: XXX
# Time per request: XXX ms
# Failed requests: 0
```

### Profiling with JFR (Java Flight Recorder)

```bash
# Start with JFR enabled
java -XX:+UnlockCommercialFeatures \
     -XX:+FlightRecorder \
     -XX:StartFlightRecording=delay=5s,duration=30s,filename=myrecording.jfr \
     -jar target/ingestion-api-1.0.0-SNAPSHOT.jar

# Open recording in JDK Mission Control
jmc myrecording.jfr
```

---

## Code Standards

### Java Style Guide

- **Naming:** camelCase for variables, PascalCase for classes
- **Logging:** Use SLF4J `logger` not `System.out.println()`
- **Annotations:** One per line for method parameters
- **Error Handling:** Use service layer exceptions, not null checks

### Commit Messages

```
feat: add RateLimitFilter configuration
fix: prevent IP address PII exposure
docs: update API reference for batch endpoint
refactor: extract Kafka producer logic
test: add EventControllerIntegrationTest cases
```

---

## Release Checklist

Before deploying to production:

- [ ] All tests pass: `mvn clean verify`
- [ ] No code warnings/errors: `mvn spotbugs:check`
- [ ] Code review completed
- [ ] Configuration updated for environment
- [ ] Documentation updated
- [ ] Performance benchmarks acceptable (p99 < 10ms for ingestion)
- [ ] Load test passed (1000+ concurrent)
- [ ] Security audit completed (OWASP Top 10)
- [ ] MongoDB indexes created
- [ ] Connection pool tuned for expected load
