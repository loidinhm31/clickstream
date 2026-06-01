#!/bin/bash
# Clickstream E2E Test Wrapper Script
#
# Requires all services running (make start-all).
# Restarts raw-archiver with FLUSH_INTERVAL_SECONDS=10 for faster parquet verification.
# Usage: bash scripts/run-e2e.sh [playwright args]

set -euo pipefail

YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

# Reduce raw-archiver flush interval for faster parquet verification (plan requirement).
# SPARK_BATCH_INTERVAL is already set to 10 in docker-compose.yml — no override needed.
export FLUSH_INTERVAL_SECONDS=10

RAW_ARCHIVER_PORT=9053
PID_DIR=.pids
ARCHIVER_PID="${PID_DIR}/raw-archiver.pid"
ARCHIVER_LOG="logs/raw-archiver.log"

echo -e "${YELLOW}Starting E2E Test Suite...${NC}"
echo -e "  FLUSH_INTERVAL_SECONDS=${FLUSH_INTERVAL_SECONDS} (raw-archiver will be restarted with this value)"

# 1. Verify infrastructure is running
echo -e "\n${YELLOW}[1/5] Checking Infrastructure...${NC}"
bash scripts/verify-setup.sh

# 2. Restart raw-archiver with reduced flush interval so parquet files appear within 90s timeout
echo -e "\n${YELLOW}[2/5] Restarting Raw Archiver with flush interval=${FLUSH_INTERVAL_SECONDS}s...${NC}"
# Kill by PID file if tracked
if [ -f "${ARCHIVER_PID}" ] && ps -p "$(cat "${ARCHIVER_PID}")" > /dev/null 2>&1; then
    echo "  Stopping existing raw-archiver (PID: $(cat "${ARCHIVER_PID}"))..."
    kill -15 "$(cat "${ARCHIVER_PID}")" 2>/dev/null || true
    rm -f "${ARCHIVER_PID}"
    sleep 3
fi
# Kill any untracked process still holding port 9053
STALE_PID=$(lsof -ti tcp:"${RAW_ARCHIVER_PORT}" 2>/dev/null || true)
if [ -n "${STALE_PID}" ]; then
    echo "  Killing stale process on port ${RAW_ARCHIVER_PORT} (PID: ${STALE_PID})..."
    kill -15 "${STALE_PID}" 2>/dev/null || true
    sleep 3
fi

mkdir -p "${PID_DIR}" logs
DATA_LAKE_PATH=./data-lake java \
    -Dserver.port="${RAW_ARCHIVER_PORT}" \
    -Darchiver.flush.time-interval-seconds="${FLUSH_INTERVAL_SECONDS}" \
    -jar raw-archiver/target/raw-archiver-1.0.0-SNAPSHOT.jar \
    > "${ARCHIVER_LOG}" 2>&1 &
echo $! > "${ARCHIVER_PID}"

echo -n "  Waiting for raw-archiver to start"
for i in $(seq 1 30); do
    if curl -sf "http://localhost:${RAW_ARCHIVER_PORT}/actuator/health" > /dev/null 2>&1; then
        echo -e "\n  ${GREEN}✓ Raw Archiver ready${NC}"
        break
    fi
    echo -n "."
    sleep 2
    if [ "$i" -eq 30 ]; then
        echo -e "\n  ${RED}✗ Raw Archiver did not start in time${NC}"
        exit 1
    fi
done

# 3. Check if E2E directory exists and dependencies are installed
echo -e "\n${YELLOW}[3/5] Preparing E2E Environment...${NC}"
if [ ! -d "tests/e2e-playwright/node_modules" ]; then
    echo "Installing E2E dependencies..."
    (cd tests/e2e-playwright && npm install)
fi

# 4. Run Playwright tests
echo -e "\n${YELLOW}[4/5] Running Playwright E2E Tests...${NC}"
TEST_EXIT=0
(cd tests/e2e-playwright && npx playwright test "$@") || TEST_EXIT=$?

# 5. Summary
echo -e "\n${YELLOW}[5/5] Summary${NC}"
if [ "${TEST_EXIT}" -eq 0 ]; then
    echo -e "${GREEN}✓ E2E Tests Passed!${NC}"
else
    echo -e "${RED}✗ E2E Tests Failed! (exit ${TEST_EXIT})${NC}"
    echo -e "  Run: cd tests/e2e-playwright && npx playwright show-report"
    exit "${TEST_EXIT}"
fi
