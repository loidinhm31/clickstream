#!/bin/bash
# Clickstream Stack Verification Script
# Checks health of infrastructure services required by local development and E2E.

set -euo pipefail

YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}Starting Clickstream Stack Verification...${NC}"

if [ -z "${XDG_RUNTIME_DIR:-}" ] && [ -d "/run/user/$(id -u)" ]; then
    export XDG_RUNTIME_DIR="/run/user/$(id -u)"
fi

resolve_container_cli() {
    if [ -n "${DOCKER_BIN:-}" ]; then
        echo "${DOCKER_BIN}"
        return 0
    fi

    if command -v docker > /dev/null 2>&1; then
        echo "docker"
        return 0
    fi

    if command -v podman > /dev/null 2>&1; then
        echo "podman"
        return 0
    fi

    return 1
}

CONTAINER_CLI="$(resolve_container_cli || true)"

run_container_cmd() {
    if [ -z "${CONTAINER_CLI}" ]; then
        echo -e "${RED}FAILED (missing docker/podman CLI)${NC}" >&2
        exit 1
    fi

    "${CONTAINER_CLI}" "$@"
}

# Helper to check container health
check_container() {
    local name=$1
    echo -n "Checking $name... "
    status=$(run_container_cmd inspect -f '{{.State.Health.Status}}' "$name" 2>/dev/null || true)
    if [ "$status" == "healthy" ]; then
        echo -e "${GREEN}HEALTHY${NC}"
    else
        echo -e "${RED}FAILED ($status)${NC}"
        return 1
    fi
}

# 1. Infrastructure
echo -e "\n${YELLOW}[1/3] Infrastructure Verification${NC}"
check_container "kafka" || exit 1
check_container "mongodb" || exit 1
check_container "kafka-ui" || exit 1

# 2. Functional Checks
echo -e "\n${YELLOW}[2/3] Functional Verification${NC}"

echo -n "Checking Kafka topic 'clickstream-events'... "
if run_container_cmd exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9094 --list | grep -q "clickstream-events"; then
    echo -e "${GREEN}FOUND${NC}"
else
    echo -e "${RED}NOT FOUND${NC}"
    exit 1
fi

echo -n "Checking MongoDB connectivity... "
if run_container_cmd exec mongodb mongosh --eval 'db.adminCommand("ping")' | grep -q "{ ok: 1 }"; then
    echo -e "${GREEN}CONNECTED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# 3. Application Services
echo -e "\n${YELLOW}[3/3] Application Services${NC}"
echo "Application services are verified and auto-started by scripts/run-e2e.sh."

echo -e "\n${GREEN}✓ Clickstream infrastructure is ready for local development and E2E.${NC}"
