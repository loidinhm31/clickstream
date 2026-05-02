#!/bin/bash
# Clickstream Stack Verification Script
# Checks health of all infrastructure and application services

set -euo pipefail

YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}Starting Clickstream Stack Verification...${NC}"

# Helper to check container health
check_container() {
    local name=$1
    echo -n "Checking $name... "
    status=$(sg docker -c "docker inspect -f '{{.State.Health.Status}}' $name" 2>/dev/null)
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
if sg docker -c "docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9094 --list" | grep -q "clickstream-events"; then
    echo -e "${GREEN}FOUND${NC}"
else
    echo -e "${RED}NOT FOUND${NC}"
    exit 1
fi

echo -n "Checking MongoDB connectivity... "
if sg docker -c "docker exec mongodb mongosh --eval 'db.adminCommand(\"ping\")'" | grep -q "{ ok: 1 }"; then
    echo -e "${GREEN}CONNECTED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# 3. Application Services (Placeholder for future phases)
echo -e "\n${YELLOW}[3/3] Application Services (Coming in Phase 02/03)${NC}"
echo "To be implemented as services are deployed."

echo -e "\n${GREEN}✓ Clickstream Infrastructure is ready!${NC}"
