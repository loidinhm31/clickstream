---
title: "Clickstream Analytics Application"
description: "Full-stack clickstream pipeline: React → Spring Boot → Kafka → Spark/Arrow/Parquet → MongoDB + Data Lake"
status: pending
priority: P1
effort: 40h
branch: main
tags: [feature, backend, frontend, infra, kafka, spark, arrow]
created: 2026-04-09
---

# Clickstream Analytics Application

## Overview

End-to-end clickstream analytics system capturing user micro-events from a React frontend, ingesting via Spring Boot into Kafka, then processing through three independent consumer groups: Spark ETL (→ MongoDB), Real-time Analytics (Arrow in-memory → Arrow Flight → frontend), and Raw Archiver (→ Parquet data lake).

## Architecture Diagram

See inline diagram above in chat, or `research/researcher-01-report.md` for details.

## Validation Summary

**Validated:** 2026-04-14
**Questions asked:** 4

### Confirmed Decisions
- **Storage Strategy:** Local Filesystem confirmed for MVP to ensure simplicity and ease of debugging.
- **Schema Management:** JSON format approved for initial Kafka event schema to accelerate development.
- **Real-time Transport:** WebSocket confirmed for sub-second Arrow IPC push from the analytics service.
- **Partitioning Key:** sessionId validated as the partition key to balance load and preserve session ordering for Spark windowing.

### Action Items
- [x] Confirm architectural assumptions (Complete)
- [x] Begin Phase 01: Dev environment setup (Complete)
- [x] Phase 02: Kafka Topic Design & Event Schema (Complete - 2026-04-18)

## Phase 2 Completion Summary

**Completed:** 2026-04-18

### Deliverables
- **EventType enum** — CLICK, PAGE_VIEW, SCROLL, HOVER event types
- **EventMetadata class** — Immutable, builder pattern with version tracking
- **ClickEvent model** — Immutable domain object with Jackson annotations, schemaVersion 1.0
- **EventValidator** — XSS, PII, URL, and length validation with pre-compiled regex patterns
- **KafkaProducerExample** — Demonstrates sessionId partition key strategy for load balancing
- **Test Suite** — 55 unit tests (12 serialization + 43 validation), all passing
- **Maven project structure** — Complete with Spring Boot, Kafka, and testing dependencies

### Code Improvements from Review
- **Immutable design** — final fields, no setters, Builder pattern for construction
- **equals/hashCode implementation** — Proper object equality for domain models
- **Pre-compiled regex patterns** — Performance optimization for validation rules
- **Configurable validation windows** — Flexible validation constraints
- **Thread-safe Builder pattern** — Safe concurrent object construction

## Phases

| # | Phase | Status | Effort | Link |
|---|-------|--------|--------|------|
| 1 | Dev environment (Docker Compose) | Done | 3h | [phase-01](./phase-01-dev-environment.md) |
| 2 | Kafka topic design & event schema | Done | 3h | [phase-02](./phase-02-kafka-design.md) |
| 3 | Spring Boot ingestion API | Done | 6h | [phase-03](./phase-03-ingestion-api.md) |
| 4 | Spark ETL pipeline | Done | 8h | [phase-04](./phase-04-spark-etl.md) |
| 5 | Real-time analytics service (Arrow) | Pending | 8h | [phase-05](./phase-05-realtime-analytics.md) |
| 6 | Raw event archiver | Pending | 3h | [phase-06](./phase-06-raw-archiver.md) |
| 7 | React frontend (Atomic Design) | Pending | 9h | [phase-07](./phase-07-react-frontend.md) |

## Dependencies

- Phase 1 must complete first (all services depend on Kafka + MongoDB)
- Phase 2 defines event schema used by all other phases
- Phase 3 depends on 1, 2
- Phases 4, 5, 6 depend on 1, 2, 3 (can be parallelized with each other)
- Phase 7 depends on 3 (REST endpoints) and 5 (Arrow Flight/WebSocket endpoints)

## Key Decisions

- **Kafka partitioning by sessionId** — balances load, preserves session ordering
- **Arrow IPC over HTTP/WebSocket** instead of native gRPC Flight — browsers can't do native gRPC
- **Embedded Spark in Spring Boot for dev** — standalone submit for prod
- **foreachBatch sink to MongoDB** — more control than Mongo Spark Connector streaming
- **Kafbat UI** for Kafka inspection during development

## Multi-Module Maven Architecture

**Restructured:** 2026-04-18

The project has been refactored from a monolithic structure to a Maven multi-module monorepo to accommodate the five planned services while avoiding codebase complexity. This ensures:
- **Clean service boundaries** — Each service (ingestion-api, spark-etl, realtime-analytics, raw-archiver) has its own directory
- **Shared model reuse** — Common event models, validators, and Kafka utilities live in `shared-models/` module
- **Centralized dependency management** — Parent POM manages Spring Boot, Kafka, Spark, Arrow versions
- **Independent builds** — Each module can be built/tested in isolation with `mvn -pl <module-name>`
- **Future-ready** — Prepared for microservices deployment if needed

### Module Structure

```
clickstream/                               # Parent POM (dependency management)
├── pom.xml                                # Spring Boot 3.2.4, Kafka 3.7.0, Spark 3.5.1, Arrow 15.0.2
├── .mvn/settings.xml                      # Maven Central mirror (corporate Artifactory bypass)
│
├── shared-models/                         # ✅ Phase 2 deliverables
│   ├── pom.xml                            # Child module (depends on parent)
│   └── src/main/java/com/clickstream/
│       ├── model/                         # EventType, EventMetadata, ClickEvent
│       ├── validation/                    # EventValidator (XSS/PII/URL validation)
│       └── kafka/                         # KafkaProducerExample (partition key)
│
├── ingestion-api/                         # ✅ Phase 3: Spring Boot REST API
│   └── (depends on shared-models)
│
├── spark-etl/                             # 🔜 Phase 4: Spark Structured Streaming → MongoDB
│   └── (depends on shared-models)
│
├── realtime-analytics/                    # 🔜 Phase 5: Arrow in-memory + WebSocket
│   └── (depends on shared-models)
│
├── raw-archiver/                          # 🔜 Phase 6: Parquet writer → Data Lake
│   └── (depends on shared-models)
│
└── frontend/                              # 🔜 Phase 7: React application
    └── (separate build system - npm/Vite)
```

### Build Commands

```bash
# From root directory
mvn clean install            # Build all modules
mvn test                     # Test all modules
mvn test -pl shared-models   # Test specific module
```

**Rationale:** As noted by user, "we will have many services to run, for that each service should in one folder/directory to avoid messy codebase." This structure scales to 5+ services without losing organization or clarity.

## Phase 4 Completion Summary

**Completed:** 2026-04-18

### Deliverables
- **Spark Structured Streaming ETL Pipeline** — Production-grade stream processing with 3 parallel output aggregations
- **SessionAggregator** — Windowed aggregation (10s tumbling window) for session-level metrics with state store
- **PageMetricsAggregator** — Page-level metrics computation with sliding windows and late-arriving data handling
- **UserJourneyBuilder** — Session-ordered event sequence building with stateful transformation
- **MongoForeachBatchWriter** — Custom sink with upsert semantics, retry logic, and batch monitoring
- **MongoDB Integration** — Automatic index creation via MongoIndexService, optimized for read patterns
- **Graceful Shutdown** — Streaming query lifecycle management with configurable timeout
- **Performance Monitoring** — StreamingQueryMonitor for query latency, throughput, and error tracking
- **Security Configuration** — Environment-based externalization for Mongo credentials and Spark settings
- **Maven Multi-Module** — spark-etl module with parent POM dependency management
- **Comprehensive Test Suite** — 5 test classes covering transformers, schema validation, and end-to-end integration

### Architecture Highlights
- **Input:** Kafka topic (clickstream) consumed at 50k events/second throughput expectation
- **Processing:** 3 parallel streaming DataFrames with independent aggregation logic
- **Output:** 3 MongoDB collections (session_aggregates, page_metrics, user_journeys) with streaming writes
- **Failure Handling:** Exponential backoff retry (3 attempts, 1s base delay) for transient MongoDB failures
- **Monitoring:** Per-batch metrics published to StreamingQueryMonitor for dashboarding

### Code Quality
- **Immutable domain models** with Jackson serialization
- **Pre-compiled Spark SQL patterns** for schema validation
- **Thread-safe configuration** via SparkConfig and MongoConfig singletons
- **Explicit error propagation** from sink to application lifecycle
- **Unit test coverage** for all transformation logic with mocked Spark sessions
