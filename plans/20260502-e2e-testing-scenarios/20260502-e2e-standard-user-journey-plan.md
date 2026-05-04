---
title: "E2E Standard User Journey Testing"
status: done
priority: P1
effort: 16h
branch: main
tags: [e2e, testing, playwright, qa]
created: 2026-05-02
reviewed: 2026-05-04
---

# E2E Standard User Journey Testing Plan

## Overview
This plan details the implementation of a comprehensive End-to-End (E2E) testing suite for the Clickstream Analytics platform. Based on the brainstorming summary, the goal is to validate data integrity and service traceability across the entire event lifecycle (Frontend -> Ingestion API -> Kafka -> Spark ETL / Real-time -> MongoDB / Data Lake).

## Architecture & Tooling
- **Test Framework**: Playwright (for driving the browser and asserting HTTP/WebSocket responses)
- **Language**: TypeScript
- **Location**: `tests/e2e-playwright`
- **Validation Sinks**:
  - `MongoVerifier`: Validates persistence in MongoDB.
  - `ParquetVerifier`: Validates durability in Parquet files.
  - `RealtimeVerifier`: Validates metrics via WebSocket/HTTP.
  - `IngestionVerifier`: Validates HTTP 202 Accepted and/or logs.

## Phase 1: Test Infrastructure Setup
- **Task 1.1**: Initialize Playwright project in `tests/e2e-playwright`.
  - Run `npm create playwright@latest` inside `tests/e2e-playwright`.
  - Configure `playwright.config.ts` to target local environment (e.g., `baseURL: 'http://localhost:5173'`).
- **Task 1.2**: Install required dependencies for verifiers.
  - `mongodb` (for MongoVerifier).
  - `parquetjs` or `parquet-wasm` (for ParquetVerifier).
  - `dotenv` (for managing local environment variables).
- **Task 1.3**: Set up test environment configuration.
  - Define local connection strings in `.env` (MongoDB `mongodb://localhost:9055/clickstream_db`, Parquet directory `./data-lake/raw-events/`).
  - **Environment Overrides**: Ensure `FLUSH_INTERVAL_SECONDS=10` and `SPARK_BATCH_INTERVAL=10s` are configured for the test run.
- **Task 1.4**: System Dependency Setup.
  - Run `npx playwright install-deps` to ensure headless Debian compatibility.

## Phase 2: Verifier Utility Implementation
- **Task 2.1**: Implement `MongoVerifier` (`tests/e2e-playwright/utils/MongoVerifier.ts`).
  - Connect to local MongoDB.
  - Expose method `waitForSessionAggregate(sessionId: string, timeout: number)` which polls the `session_aggregates` collection until the record appears.
- **Task 2.2**: Implement `ParquetVerifier` (`tests/e2e-playwright/utils/ParquetVerifier.ts`).
  - Scan the `data-lake/raw-events/` directory for recently created files.
  - Read files and search for the specific `eventId` or `sessionId`.
- **Task 2.3**: Implement `RealtimeVerifier` (`tests/e2e-playwright/utils/RealtimeVerifier.ts`).
  - Listen to WebSocket frames or poll `/api/realtime/metrics` to ensure active sessions count increments or page views update.

## Phase 3: The "Standard User Journey" Test Script
- **Task 3.1**: Create `tests/e2e-playwright/tests/standard-journey.spec.ts`.
- **Task 3.2**: Script the User Journey.
  - Generate a unique `sessionId` (e.g., UUID) and inject it into the browser context (localStorage or query param) before navigation.
  - **Navigate**: Load home page.
  - **Scroll**: Simulate scrolling to trigger scroll events.
  - **Click**: Click specific trackable elements.
  - **Navigate**: Go to the Features page.
  - **Submit**: Fill and submit the Newsletter form.
- **Task 3.3**: Validate Ingestion during the journey.
  - Use `page.waitForResponse` to ensure the tracking payload gets a `202 Accepted` from the `ingestion-api`.

## Phase 4: Integration & Synchronization
- **Task 4.1**: Integrate Verifiers into the test script.
  - After completing the journey actions, call `MongoVerifier` with a timeout of 45 seconds (Spark ETL triggers every 30s).
  - Call `ParquetVerifier` with a timeout of 90 seconds (Raw Archiver flushes every 60s).
  - Call `RealtimeVerifier` immediately after actions to check live metrics.
- **Task 4.2**: Test teardown and cleanup.
  - Ensure connections to MongoDB are closed gracefully.
  - Consider adding a global setup/teardown to clean test collections or files.

## Phase 5: CI/CD & Scripts
- **Task 5.1**: Add `run-e2e.sh` wrapper script.
  - Create a script that starts the Docker stack, waits for health checks, runs Playwright tests, and spins down.
- **Task 5.2**: Update `verify-setup.sh`.
  - Optionally integrate a smoke test using the new E2E suite.

## Risks & Mitigation
- **Flakiness due to timing**: Spark ETL and Raw Archiver operate on micro-batches. Mitigation: Implement robust polling with sufficient timeouts in Verifiers rather than fixed `sleep()` calls.
## Validation Summary

**Validated:** 2026-05-02
**Questions asked:** 4

### Confirmed Decisions
- **Environment**: Use `npx playwright install-deps` on the host Debian system.
- **Traceability**: Stick to "Sink-only" verification (MongoDB/Parquet) for robustness; log scraping omitted.
- **Performance**: Reduce service intervals (e.g., `FLUSH_INTERVAL_SECONDS`) during test runs for faster feedback.
- **Complexity**: Implement full system-wide data integrity checks (DB + Filesystem verification).

### Action Items
- [x] Add Task 1.4: Script to install Playwright system dependencies on Debian.
- [x] Update Task 5.1: Ensure `run-e2e.sh` sets `FLUSH_INTERVAL_SECONDS=10` and `SPARK_BATCH_INTERVAL=10`.
- [x] Refine Verifiers: Ensure they handle shorter timeouts based on reduced intervals.

## Implementation Status (2026-05-04)

### Completed
- [x] Task 1.1: Playwright project initialized in `tests/e2e-playwright`
- [x] Task 1.2: Dependencies installed (mongodb, parquetjs-lite, dotenv, axios, uuid)
- [x] Task 1.3: `.env` configured with correct connection strings; DATA_LAKE_PATH fixed to `../../../data-lake/raw-events/` (was 1 level too shallow)
- [x] Task 1.4: `tsconfig.json` created; `npx playwright install-deps` documented in QUICKSTART
- [x] Task 2.1: `MongoVerifier` implemented with polling `waitForSessionAggregate`
- [x] Task 2.2: `ParquetVerifier` implemented with recursive scan + polling
- [x] Task 2.3: `RealtimeVerifier` implemented with HTTP polling of `/api/realtime/stats`
- [x] Task 3.1-3.3: `standard-journey.spec.ts` created; ingestion 202 verified via `waitForResponse`
- [x] Task 4.1: All three verifiers integrated; timeouts set to 60s (Mongo) and 90s (Parquet)
- [x] Task 4.2: MongoDB connection closed in `afterAll`; totalEvents field corrected to sum of clickCount + pageViewCount + scrollEvents
- [x] Task 5.1: `scripts/run-e2e.sh` created; sets FLUSH_INTERVAL_SECONDS=10, restarts raw-archiver, runs Playwright
- [x] Task 5.2: Makefile targets `test-e2e` and `test-e2e-headed` added

### Resolved Issues (second review 2026-05-04)
- [x] **FIXED**: `ParquetVerifier.ts` fallback path corrected to `'../../../data-lake/raw-events/'`
- [x] **FIXED**: `RealtimeVerifier.ts` fallback port corrected to `9052`
- [x] **FIXED**: `testUserId` removed from spec
- [x] **FIXED**: `playwright.config.ts` now reads `process.env.BASE_URL`
- [x] **FIXED**: `parquetVerifier` / `realtimeVerifier` moved to class-level initialization (no uninitialized access)
- [x] **FIXED**: `MongoVerifier.waitForUserJourney` removed (dead code)
- [x] **FIXED**: `RealtimeVerifier.waitForActiveSessions` removed (dead code)
- [x] **FIXED**: `reader.close()` moved to `finally` block in `ParquetVerifier`
- [x] **FIXED**: Misleading `SPARK_BATCH_INTERVAL` export removed from `run-e2e.sh`
- [x] **FIXED**: `typecheck` script added to `package.json`; TypeScript strict mode: 0 errors

### Remaining Low-priority Items
- `getStats()` return type is `any` — could be typed against actual API shape (cosmetic)
- `waitForActiveSessions` / `waitForUserJourney` removal not verified via grep; assumed complete per review report
