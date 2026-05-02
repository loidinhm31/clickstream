---
title: "Phase 02: Core Services & Ingestion"
description: "Building the shared model library and launching the Ingestion API"
status: completed
priority: P1
effort: 2h
branch: main
tags: [shared-models, ingestion-api, maven, spring-boot]
created: 2026-05-01
---

# Phase 02: Core Services & Ingestion

Building the shared model library and launching the Ingestion API.

## 1. Shared Models (Dependency)

The `shared-models` package contains the `ClickEvent` POJO and common constants used by all Java services.

**Build and Install:**
```bash
cd shared-models
mvn clean install
```
*Note: This must be done before building any other Java service.*

## 2. Ingestion API

The Spring Boot entry point for all clickstream events.

### Dependencies
Added `spring-boot-starter-actuator` to `ingestion-api/pom.xml` for health monitoring and metrics.

**Configuration:**
Ensure `ingestion-api/src/main/resources/application.yml` points to:
- Kafka: `localhost:9056`
- MongoDB: `localhost:9055`

**Run locally:**
```bash
cd ingestion-api
mvn spring-boot:run
```

**Run JAR:**
```bash
cd ingestion-api
mvn clean package
java -jar target/ingestion-api-0.0.1-SNAPSHOT.jar
```

**Verification:**
```bash
curl http://localhost:9051/actuator/health
```

### Testing and Maintenance
The `MetricsEngine.reset()` method is available for clearing metrics during testing and maintenance cycles.

## Summary Checklist
- [x] `shared-models` installed to local Maven repository
- [x] `ingestion-api` starts without errors
- [x] API successfully connects to Kafka and MongoDB
- [x] Health endpoint returns `{"status":"UP"}`
- [x] Actuator added for monitoring
- [x] `MetricsEngine.reset()` validated for test resets
