---
title: "Service Orchestration Plan"
status: in-progress
priority: P1
effort: 4h
branch: main
tags: [orchestration, docker, kafka, mongodb, spark]
created: 2026-05-01
---

# Service Orchestration Plan

This plan outlines the orchestration and startup sequence for the Clickstream Analytics platform. It ensures that infrastructure, core services, processing pipelines, and the frontend are initialized in the correct order to handle data flow from ingestion to visualization.

## Dependency Graph

```mermaid
graph TD
    subgraph Infrastructure
        Kafka[Apache Kafka]
        MongoDB[(MongoDB)]
    end

    subgraph Core
        SharedModels[Shared Models]
    end

    subgraph Processing
        SparkETL[Spark ETL]
        RawArchiver[Raw Archiver]
        RealtimeAnalytics[Real-time Analytics]
    end

    subgraph Ingestion
        IngestionAPI[Ingestion API]
    end

    subgraph UI
        Frontend[React Frontend]
    end

    %% Dependencies
    SharedModels -.-> IngestionAPI
    SharedModels -.-> SparkETL
    SharedModels -.-> RawArchiver
    SharedModels -.-> RealtimeAnalytics

    IngestionAPI --> Kafka
    SparkETL --> Kafka
    SparkETL --> MongoDB
    RawArchiver --> Kafka
    RealtimeAnalytics --> Kafka

    Frontend --> IngestionAPI
    Frontend --> RealtimeAnalytics
    IngestionAPI --> MongoDB
```

## Startup Phases

| Phase | Description | Key Services | Status |
|-------|-------------|--------------|--------|
| [Phase 01](./phase-01-infrastructure.md) | Infrastructure Layer | Kafka, MongoDB, Kafka UI | ✅ DONE (2026-05-01 22:42) |
| [Phase 02](./phase-02-core-services.md) | Core & Ingestion | Shared Models, Ingestion API | ✅ DONE (2026-05-01 23:30) |
| [Phase 03](./phase-03-processing-analytics.md) | Data Processing | Spark ETL, Real-time Analytics, Raw Archiver | Pending |
| [Phase 04](./phase-04-frontend.md) | User Interface | React Frontend | Pending |

## Orchestration Logic

The optimal startup order is:
1. **Infrastructure**: Start Kafka and MongoDB first. Ensure health checks pass.
2. **Initialization**: Build `shared-models` as it is a dependency for all Java services.
3. **Processing Sinks**: Start `spark-etl` so it's ready to process incoming events and populate MongoDB.
4. **Analytics & Archival**: Start `realtime-analytics` and `raw-archiver`.
5. **Ingestion**: Start `ingestion-api` to begin accepting traffic.
6. **UI**: Start the `frontend` once all backend services are healthy.

## Health Verification

A verification script `scripts/verify-setup.sh` should be used to ensure all components are responding correctly.

- **Kafka**: `kafka-broker-api-versions.sh`
- **MongoDB**: `mongosh --eval "db.adminCommand('ping')"`
- **Ingestion API**: `GET /actuator/health`
- **Real-time Analytics**: `GET /api/realtime/health`
- **Raw Archiver**: `GET /actuator/health`

## Validation Summary

**Validated:** 2026-05-01
**Questions asked:** 4

### Confirmed Decisions
- **Spark ETL Execution**: Dockerized - Use Docker to manage Spark environment and dependencies.
- **Port Strategy**: 905x Range - Use the 9050-9056 range to avoid local port conflicts.
- **Development Workflow**: Hybrid - Infrastructure in Docker; Frontend and Ingestion API run natively for faster iteration.
- **Automation**: Include Verification Script - Create `scripts/verify-setup.sh` to automate health checks.

### Action Items
- [x] Implement `scripts/verify-setup.sh` for infrastructure health checks.
- [x] Ensure `docker-compose.yml` matches the 905x port range for infrastructure services.
- [ ] Expand `scripts/verify-setup.sh` for application services (Phase 02/03).
- [ ] Verify `frontend` and `ingestion-api` local configs use the validated port range.
