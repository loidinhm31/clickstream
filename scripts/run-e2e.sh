#!/bin/bash
# Clickstream E2E Test Wrapper Script
#
# Requires infrastructure running (Kafka, MongoDB, Kafka UI, Spark ETL).
# Starts missing host application services, restarts raw-archiver with a short
# flush interval, then runs the Playwright suite.
# Usage: bash scripts/run-e2e.sh [playwright args]

set -euo pipefail

YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Reduce raw-archiver flush interval for faster parquet verification (plan requirement).
# SPARK_BATCH_INTERVAL is already set to 10 in docker-compose.yml — no override needed.
export FLUSH_INTERVAL_SECONDS=10

RAW_ARCHIVER_PORT=9053
INGESTION_API_PORT=9051
REALTIME_ANALYTICS_PORT=9052
FRONTEND_PORT=9059
PID_DIR=.pids
INGESTION_PID="${PID_DIR}/ingestion-api.pid"
REALTIME_PID="${PID_DIR}/realtime-analytics.pid"
ARCHIVER_PID="${PID_DIR}/raw-archiver.pid"
FRONTEND_PID="${PID_DIR}/frontend.pid"
INGESTION_LOG="logs/ingestion-api.log"
REALTIME_LOG="logs/realtime-analytics.log"
ARCHIVER_LOG="logs/raw-archiver.log"
FRONTEND_LOG="logs/frontend.log"

print_log_excerpt() {
    local logfile=$1

    if [ -f "${logfile}" ]; then
        echo -e "${YELLOW}Recent log output from ${logfile}:${NC}"
        tail -n 40 "${logfile}"
    fi
}

pid_is_running() {
    local pidfile=$1

    [ -f "${pidfile}" ] && ps -p "$(cat "${pidfile}")" > /dev/null 2>&1
}

remove_stale_pidfile() {
    local pidfile=$1

    if [ -f "${pidfile}" ] && ! ps -p "$(cat "${pidfile}")" > /dev/null 2>&1; then
        rm -f "${pidfile}"
    fi
}

find_port_pid() {
    local port=$1

    (lsof -ti tcp:"${port}" 2>/dev/null | head -n 1) || true
}

kill_port_listener() {
    local port=$1
    local pid

    pid=$(find_port_pid "${port}")
    if [ -n "${pid}" ]; then
        echo "  Killing existing listener on port ${port} (PID: ${pid})..."
        kill -15 "${pid}" 2>/dev/null || true
        sleep 2
    fi
}

wait_for_http() {
    local name=$1
    local url=$2
    local timeout_seconds=$3
    local logfile=$4
    local attempts=$((timeout_seconds / 2))

    if [ "${attempts}" -lt 1 ]; then
        attempts=1
    fi

    echo -n "  Waiting for ${name} at ${url}"
    for i in $(seq 1 "${attempts}"); do
        if curl -sf "${url}" > /dev/null 2>&1; then
            echo -e "\n  ${GREEN}✓ ${name} ready${NC}"
            return 0
        fi

        echo -n "."
        sleep 2
    done

    echo -e "\n  ${RED}✗ ${name} did not become ready in ${timeout_seconds}s${NC}"
    print_log_excerpt "${logfile}"
    return 1
}

start_background_service() {
    local name=$1
    local port=$2
    local pidfile=$3
    local logfile=$4
    local command=$5

    remove_stale_pidfile "${pidfile}"
    mkdir -p "${PID_DIR}" logs

    if pid_is_running "${pidfile}"; then
        echo "  Restarting tracked ${name} process ($(cat "${pidfile}"))..."
        kill -15 "$(cat "${pidfile}")" 2>/dev/null || true
        rm -f "${pidfile}"
        sleep 2
    fi

    kill_port_listener "${port}"

    echo "  Starting ${name}..."
    bash -lc "${command}" > "${logfile}" 2>&1 &
    echo $! > "${pidfile}"
}

ensure_http_service() {
    local name=$1
    local port=$2
    local pidfile=$3
    local logfile=$4
    local url=$5
    local command=$6

    if curl -sf "${url}" > /dev/null 2>&1; then
        echo "  ${name} already ready."
        return 0
    fi

    start_background_service "${name}" "${port}" "${pidfile}" "${logfile}" "${command}"
    wait_for_http "${name}" "${url}" 60 "${logfile}"
}

detect_playwright_browser() {
    local browser_cache browser_path

    if [ -n "${PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH:-}" ]; then
        return 0
    fi

    browser_cache="${HOME}/.cache/ms-playwright"
    browser_path=$( (find "${browser_cache}" -maxdepth 3 \
        -path '*/chromium_headless_shell-*/chrome-headless-shell-linux64/chrome-headless-shell' \
        -type f -perm -111 2>/dev/null | sort -V | tail -n 1) || true )

    if [ -n "${browser_path}" ]; then
        export PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH="${browser_path}"
        echo -e "  Using cached Chromium fallback: ${PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH}"
    fi
}

echo -e "${YELLOW}Starting E2E Test Suite...${NC}"
echo -e "  FLUSH_INTERVAL_SECONDS=${FLUSH_INTERVAL_SECONDS} (raw-archiver will be restarted with this value)"

# 1. Verify infrastructure is running
echo -e "\n${YELLOW}[1/5] Checking Infrastructure...${NC}"
bash scripts/verify-setup.sh

# 2. Ensure host application services are up
echo -e "\n${YELLOW}[2/5] Ensuring Host Application Services...${NC}"
ensure_http_service \
    "Ingestion API" \
    "${INGESTION_API_PORT}" \
    "${INGESTION_PID}" \
    "${INGESTION_LOG}" \
    "http://127.0.0.1:${INGESTION_API_PORT}/actuator/health" \
    "cd ingestion-api && exec mvn spring-boot:run -Dspring-boot.run.jvmArguments='-Dserver.port=${INGESTION_API_PORT}'"

ensure_http_service \
    "Real-time Analytics" \
    "${REALTIME_ANALYTICS_PORT}" \
    "${REALTIME_PID}" \
    "${REALTIME_LOG}" \
    "http://127.0.0.1:${REALTIME_ANALYTICS_PORT}/api/realtime/health" \
    "cd realtime-analytics && exec mvn spring-boot:run -Dspring-boot.run.jvmArguments='--add-opens=java.base/java.nio=ALL-UNNAMED -Dserver.port=${REALTIME_ANALYTICS_PORT}'"

ensure_http_service \
    "Frontend" \
    "${FRONTEND_PORT}" \
    "${FRONTEND_PID}" \
    "${FRONTEND_LOG}" \
    "http://127.0.0.1:${FRONTEND_PORT}" \
    "cd frontend && exec npm run dev"

# 3. Restart raw-archiver with reduced flush interval so parquet files appear within 90s timeout
echo -e "\n${YELLOW}[3/5] Restarting Raw Archiver with flush interval=${FLUSH_INTERVAL_SECONDS}s...${NC}"
if pid_is_running "${ARCHIVER_PID}"; then
    echo "  Stopping existing raw-archiver (PID: $(cat "${ARCHIVER_PID}"))..."
    kill -15 "$(cat "${ARCHIVER_PID}")" 2>/dev/null || true
    rm -f "${ARCHIVER_PID}"
    sleep 3
fi
kill_port_listener "${RAW_ARCHIVER_PORT}"

start_background_service \
    "Raw Archiver" \
    "${RAW_ARCHIVER_PORT}" \
    "${ARCHIVER_PID}" \
    "${ARCHIVER_LOG}" \
    "cd '${ROOT_DIR}' && exec env DATA_LAKE_PATH=./data-lake java -Dserver.port=${RAW_ARCHIVER_PORT} -Darchiver.flush.time-interval-seconds=${FLUSH_INTERVAL_SECONDS} -jar raw-archiver/target/raw-archiver-1.0.0-SNAPSHOT.jar"
wait_for_http "Raw Archiver" "http://127.0.0.1:${RAW_ARCHIVER_PORT}/actuator/health" 60 "${ARCHIVER_LOG}"

# 4. Check if E2E directory exists and dependencies are installed
echo -e "\n${YELLOW}[4/5] Preparing E2E Environment...${NC}"
if [ ! -d "tests/e2e-playwright/node_modules" ]; then
    echo "Installing E2E dependencies..."
    (cd tests/e2e-playwright && npm install)
fi
detect_playwright_browser

# 5. Run Playwright tests
echo -e "\n${YELLOW}[5/5] Running Playwright E2E Tests...${NC}"
TEST_EXIT=0
(cd tests/e2e-playwright && npx playwright test "$@") || TEST_EXIT=$?

# Summary
echo -e "\n${YELLOW}Summary${NC}"
if [ "${TEST_EXIT}" -eq 0 ]; then
    echo -e "${GREEN}✓ E2E Tests Passed!${NC}"
else
    echo -e "${RED}✗ E2E Tests Failed! (exit ${TEST_EXIT})${NC}"
    echo -e "  Run: cd tests/e2e-playwright && npx playwright show-report"
    exit "${TEST_EXIT}"
fi
