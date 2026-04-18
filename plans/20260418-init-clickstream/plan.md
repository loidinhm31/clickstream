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
- [ ] Begin Phase 01: Dev environment setup

## Phases

| # | Phase | Status | Effort | Link |
|---|-------|--------|--------|------|
| 1 | Dev environment (Docker Compose) | Done | 3h | [phase-01](./phase-01-dev-environment.md) |
| 2 | Kafka topic design & event schema | Pending | 3h | [phase-02](./phase-02-kafka-design.md) |
| 3 | Spring Boot ingestion API | Pending | 6h | [phase-03](./phase-03-ingestion-api.md) |
| 4 | Spark ETL pipeline | Pending | 8h | [phase-04](./phase-04-spark-etl.md) |
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
