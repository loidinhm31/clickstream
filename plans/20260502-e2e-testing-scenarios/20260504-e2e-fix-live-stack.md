---
title: "E2E Live Stack Fix — Service Issues Blocking Test Run"
status: completed
priority: P0
effort: 3h
branch: main
tags: [e2e, testing, bugfix, ops]
created: 2026-05-04
parent-plan: 20260502-e2e-standard-user-journey-plan.md
---

# E2E Live Stack Fix

## Context

The Playwright E2E suite was implemented and TypeScript-verified (commit `23f5a74`). It was never executed against a live stack. A post-implementation service audit found three issues that will cause the test run to fail before any assertion is reached, or fail specific assertions.

**Entry point to run tests:**
```bash
bash scripts/run-e2e.sh   # or: make test-e2e
```

**TypeScript check (already passing):**
```bash
cd tests/e2e-playwright && npx tsc --noEmit
```

---

## Current Stack State (as of 2026-05-04)

| Service | Port | Status | Notes |
|---|---|---|---|
| Kafka | 9056 | ✅ healthy | Docker |
| MongoDB | 9055 | ✅ healthy | Docker |
| Kafka-UI | 9050 | ✅ healthy | Docker |
| Spark ETL | — | ⚠️ Docker "unhealthy" | Writing to MongoDB correctly; healthcheck config wrong |
| Ingestion API | 9051 | ✅ UP | Spring Boot, host process |
| Realtime Analytics | 9052 | ⚠️ partial | Stats endpoint works; Kafka consumer reports DOWN |
| Frontend (Vite) | 9059 | ✅ running | Host process |
| Raw Archiver | 9053 | ❌ DOWN | Not running; no parquet files written |

**Kafka topic:** `clickstream-events` exists ✅  
**Data lake parquet files:** 0 ❌

---

## Issue 1 — Raw Archiver Not Running (CRITICAL)

**Symptom:**
```
GET http://localhost:9053/actuator/health → {"status":"DOWN"}
find data-lake/raw-events -name "*.parquet" | wc -l → 0
```

**Impact:** Test step 7 (`parquetVerifier.waitForSessionInParquet`, 90s timeout) will always time out and fail.

**Root cause (unconfirmed):** The archiver process crashed or was never started in the current session. `run-e2e.sh` restarts it, but something may prevent it from starting cleanly.

**Files:**
- `raw-archiver/src/main/resources/application.yml` — flush config: `time-interval-seconds: ${FLUSH_INTERVAL_SECONDS:60}`
- `scripts/run-e2e.sh` lines 25–45 — restart logic
- `logs/raw-archiver.log` — crash reason

**Investigation steps:**
```bash
# Check why it failed last time
tail -80 logs/raw-archiver.log

# Try starting manually to see error output
cd raw-archiver && FLUSH_INTERVAL_SECONDS=10 mvn spring-boot:run \
    -Dspring-boot.run.jvmArguments="-Dserver.port=9053 -Darchiver.flush.time-interval-seconds=10"

# Check health after start
curl http://localhost:9053/actuator/health
```

**Expected fix:** Identify and resolve why the process fails to start (likely a Kafka consumer group conflict, a port bind error, or a missing build artifact). Ensure `make start-all` or `make start-raw-archiver` keeps it running before `make test-e2e` is called.

**Verify fix:** At least one `.parquet` file appears under `data-lake/raw-events/year=2026/` within 15s of sending events.

---

## Issue 2 — Realtime Analytics Kafka Consumer Reporting DOWN (MEDIUM)

**Symptom:**
```
GET http://localhost:9052/api/realtime/health
→ {"kafka":"DOWN","status":"DOWN","service":"realtime-analytics",...}
```

**Impact:** The E2E test assertion is:
```typescript
expect(stats!.activeWebSocketSessions).toBeGreaterThanOrEqual(1);
```
This reads from `/api/realtime/stats` (not `/health`), and `activeWebSocketSessions` is tracked by the WebSocket handler independently of Kafka. So this assertion **may still pass** even with Kafka down, as long as the browser's WebSocket connection to the realtime service succeeds on page load.

However, if Kafka is truly disconnected, realtime analytics will not process any events, degrading the service beyond the test run.

**Files:**
- `realtime-analytics/src/main/java/com/clickstream/realtime/kafka/EventConsumer.java` — check `isHealthy()` implementation
- `realtime-analytics/src/main/resources/application.yml` — `bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9056}`
- `logs/realtime-analytics.log` — Kafka consumer errors at startup

**Investigation steps:**
```bash
# Check Kafka errors in realtime logs
grep -i "kafka\|error\|exception\|cannot connect" logs/realtime-analytics.log | tail -40

# Confirm Kafka port is reachable from host
nc -zv localhost 9056

# Check EventConsumer isHealthy() logic
cat realtime-analytics/src/main/java/com/clickstream/realtime/kafka/EventConsumer.java
```

**Likely root causes:**
1. The consumer never received a message from Kafka, so an internal "last received time" check times out and `isHealthy()` returns false even though connectivity is fine.
2. A `JsonDeserializer` type-mapping error (`ClickEvent` class not found on the classpath) causes the consumer to stop polling and mark itself unhealthy.
3. Actual Kafka connectivity failure (less likely since `nc -zv localhost 9056` should confirm).

**Expected fix:** Either correct the `isHealthy()` logic (e.g., check last poll time rather than last message received) or fix the deserialization config if events aren't being consumed.

**Verify fix:**
```bash
curl http://localhost:9052/api/realtime/health
# Should return: {"status":"UP","kafka":"UP",...}
```

---

## Issue 3 — Spark ETL Docker Health Check "unhealthy" (LOW)

**Symptom:**
```
docker compose ps → spark-etl  Up 13 hours (unhealthy)
```

**Impact:** `verify-setup.sh` does NOT check Spark ETL health, so this does not block the test run. Spark ETL is functionally writing to MongoDB (confirmed via `docker logs spark-etl`):
```
Batch 4 written to session_aggregates in 9024ms (2 rows, 0 rows/sec)
Batch 4 written to user_journeys in 8982ms (2 rows, 0 rows/sec)
Batch 4 written to page_metrics in 8606ms (2 rows, 0 rows/sec)
```
The warning `Current batch is falling behind. The trigger interval is 10000ms, but spent 10001ms` indicates the healthcheck timeout is too tight.

**Files:**
- `docker-compose.yml` — `spark-etl` service `healthcheck` block

**Investigation steps:**
```bash
# Find the healthcheck definition
grep -A 10 "spark-etl" docker-compose.yml | grep -A 6 "healthcheck"

# See how long each batch actually takes
docker logs spark-etl 2>&1 | grep "written to session_aggregates" | tail -10
```

**Expected fix:** Loosen the healthcheck — increase `timeout` to `30s` and `interval` to `60s` in `docker-compose.yml` for the `spark-etl` service so the container shows `healthy` instead of `unhealthy`.

**Verify fix:**
```bash
docker compose ps spark-etl
# Status should show: healthy
```

---

## Recommended Fix Order

```
Issue 1 (Raw Archiver) → Issue 2 (Realtime Kafka) → Issue 3 (Spark ETL healthcheck)
```

Issue 1 is the only hard blocker. Issues 2 and 3 can be fixed in parallel after the test run is unblocked.

---

## Quick Diagnostic Commands (Run First)

```bash
# Full stack health snapshot
curl -s http://localhost:9051/actuator/health | python3 -m json.tool
curl -s http://localhost:9052/api/realtime/health | python3 -m json.tool
curl -s http://localhost:9053/actuator/health | python3 -m json.tool
docker compose ps

# Raw archiver crash reason
tail -80 logs/raw-archiver.log

# Realtime Kafka errors
grep -i "kafka\|error\|exception" logs/realtime-analytics.log | tail -30

# Parquet file count
find data-lake/raw-events -name "*.parquet" | wc -l

# Spark ETL healthcheck config
grep -A 8 "healthcheck" docker-compose.yml
```

---

## Done Criteria

- [x] `bash scripts/run-e2e.sh` equivalent live-stack execution completes without error
- [x] Playwright reports all assertions in `standard-journey.spec.ts` passed
- [x] `GET http://localhost:9053/actuator/health` -> `{"status":"UP"}`
- [x] At least one `.parquet` file exists under `data-lake/raw-events/`
- [x] `GET http://localhost:9052/api/realtime/health` -> `{"status":"UP","kafka":"UP",...}`
- [x] `docker compose ps spark-etl` shows `healthy`

---

## Completion Summary (2026-05-13)

The live-stack E2E flow was verified in Docker because the host environment did not provide the required Java/Maven/Node toolchain.

**Validation results:**
- Frontend production compile passed in a `node:22` container.
- E2E TypeScript check passed in a `node:22` container.
- Playwright live-stack test passed against Docker network `clickstream_clickstream-net`.
- Ingestion API health endpoint returned `UP`.
- Realtime Analytics health endpoint returned `UP` with Kafka `UP`.
- Raw Archiver health endpoint returned `UP`.
- Spark ETL container reported `healthy`.
- Follow-up verification found raw-archiver health could become `DOWN` after idle partial batches because time-based flushing only ran when a new Kafka record arrived. Added scheduled flushing and snapshot-safe buffer removal, rebuilt raw-archiver, restarted it, and reran the Playwright live-stack test successfully.

**Implemented fixes:**
- `make start-infra` now runs Kafka topic initialization before services depend on Kafka.
- Realtime Analytics startup includes the required Arrow/JDK module opening flag.
- Vite dev server uses e2e-compatible port/proxy defaults and configurable proxy targets.
- Frontend event tracking now avoids brittle browser API assumptions, flushes observable event batches during tests, and preserves unload delivery behavior.
- Sessions and journeys pages now render the current analytics schemas used by the ingestion and aggregation services.
- Raw Archiver now has a scheduled time-interval flush path so low-volume tail batches are persisted without requiring another incoming Kafka event.
