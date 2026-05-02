---
title: "Phase 03: Processing & Analytics"
description: "Starting the downstream consumers that process Kafka events"
status: completed
priority: P1
effort: 2h
branch: main
tags: [spark, realtime-analytics, raw-archiver, kafka]
created: 2026-05-01
---

# Phase 03: Processing & Analytics

Starting the downstream consumers that process Kafka events.

## 1. Spark ETL (Dockerized)

Processes raw events into session-level aggregates in MongoDB.

**Status:** ✅ DONE & STABLE
- Fixed `awaitAnyTermination` issue where the container would exit immediately after starting.
- Verified stable operation within Docker container.

**Commands:**
```bash
# Start Spark ETL via Docker Compose
docker compose up -d spark-etl
```

**Verification:**
```bash
docker logs -f spark-etl
# Look for "Streaming query started" and MongoDB upsert logs
```

## 2. Real-time Analytics

Provides sub-second metrics via WebSockets and Apache Arrow.

**Status:** ✅ DONE
- Running locally on port 9052.
- WebSocket and Arrow streams verified.

**Build and Run:**
```bash
cd realtime-analytics
mvn clean package -DskipTests
java -jar target/realtime-analytics-0.0.1-SNAPSHOT.jar
```

**Verification:**
```bash
curl http://localhost:9052/api/realtime/health
```

## 3. Raw Archiver

Persists all raw events to Parquet files for long-term storage.

**Status:** ⚠️ BLOCKED
- Implementation complete, but currently blocked on Spring Boot 3 / Jakarta migration issues (specifically related to Kafka/Parquet dependencies and `javax.*` vs `jakarta.*` namespace conflicts).

**Build and Run:**
```bash
cd raw-archiver
mvn clean package -DskipTests
java -jar target/raw-archiver-0.0.1-SNAPSHOT.jar
```

**Verification:**
```bash
curl http://localhost:9053/actuator/health
ls -R data-lake/
```

## Summary Checklist
- [x] Spark ETL running in Docker and writing to MongoDB
- [x] Real-time Analytics accessible on port 9052
- [ ] Raw Archiver running and creating Parquet files in `data-lake/` (Blocked)
- [x] Ingestion flow verified from Ingestion API -> Kafka -> Spark/Realtime -> MongoDB
