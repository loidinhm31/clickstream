# Brainstorming Summary: End-to-End Testing Scenarios

**Date:** 2026-05-02
**Topic:** Full-Flow E2E Testing with Log Recognition & Service Traceability

## 1. Problem Statement
The Clickstream Analytics platform has multiple moving parts (Frontend, Ingestion API, Kafka, Spark ETL, Real-time Engine, Raw Archiver, MongoDB, Data Lake). We lack a unified test scenario that validates data integrity and service health across the entire lifecycle of a single event.

## 2. Requirements
- Validate event capture in the browser (Playwright).
- Verify successful ingestion (202 Accepted).
- Trace event through Kafka to multiple consumers.
- Confirm persistent state in MongoDB (Spark ETL).
- Confirm live metric updates (Real-time Engine).
- Verify long-term durability (Raw Archiver/Parquet).

## 3. Evaluated Approaches

### A. Manual Guideline-based Testing
- **Pros:** Low initial effort, no complex test infra.
- **Cons:** High human error, not repeatable, slow verification of logs across 5+ services.

### B. Automated Playwright + Log Scraping (Recommended)
- **Pros:** Highly repeatable, captures transient state (WebSockets), validates file-system side effects (Parquet).
- **Cons:** Requires environment where all logs and databases are accessible to the test runner.

## 4. Final Recommended Solution: "The Traceability Scenario"
A Playwright-driven E2E suite that triggers a multi-step user journey and then uses a set of "Verifiers" to check the state in different sinks.

### Scenario: "The Standard User Journey"
1. **Navigate** (Home) -> **Scroll** -> **Click** -> **Navigate** (Features) -> **Submit** (Newsletter).
2. **Trace ID**: Inject a unique `userId` or `sessionId` for each run.

### Validation Criteria:
- **Ingestion**: Log check in `ingestion-api` for Kafka publish.
- **Spark ETL**: MongoDB query for `session_aggregates` (match sessionId).
- **Real-time**: Capture WebSocket frame or query `/api/realtime/metrics`.
- **Durability**: Check `./data-lake/raw-events/` for a Parquet file containing the trace ID.

## 5. Implementation Considerations & Risks
- **Race Conditions**: Spark ETL has a 30s micro-batch trigger; Raw Archiver has a 60s flush trigger. Tests MUST wait for these intervals.
- **Environment**: Kafka/MongoDB must be in a known state before start (Cleanup scripts).
- **Log Access**: The test runner needs permissions to read service log files or access a centralized log API.

## 6. Success Metrics
- **Pipeline Latency**: End-to-end trace (Browser to Parquet) < 90s.
- **Data Fidelity**: 100% of event metadata (e.g., `scrollDepth`) preserved in Parquet.
- **Reliability**: Test suite passes consistently in a healthy Docker environment.

## 7. Next Steps
1. Create a dedicated test folder: `tests/e2e-playwright`.
2. Implement `EventVerifier` utility classes for MongoDB and Parquet.
3. Configure Playwright to run against the local Docker stack.
4. Integrate with CI/CD or `verify-setup.sh`.
