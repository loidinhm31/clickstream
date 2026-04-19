#!/bin/bash
set -e

echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║        Clickstream Dev Environment Verification Script            ║"
echo "╚════════════════════════════════════════════════════════════════════╝"
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}[1/8]${NC} Starting Docker Compose services..."
docker compose up -d

echo ""
echo -e "${BLUE}[2/8]${NC} Waiting for services to be healthy..."
echo "Waiting for Kafka..."
until docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9056 > /dev/null 2>&1; do
  echo -n "."
  sleep 2
done
echo -e " ${GREEN}✓${NC}"

echo ""
echo -e "${BLUE}[3/8]${NC} Checking container status..."
docker compose ps

echo ""
echo -e "${BLUE}[4/8]${NC} Listing Kafka topics..."
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9056 --list

echo ""
echo -e "${BLUE}[5/8]${NC} Describing clickstream-events topic..."
TOPIC_DESC=$(docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9056 --describe --topic clickstream-events)
echo "$TOPIC_DESC"

# Verify partition count
if echo "$TOPIC_DESC" | grep -q "PartitionCount: 6"; then
  echo -e "${GREEN}✓ Partition count verified: 6${NC}"
else
  echo -e "${RED}✗ ERROR: Expected 6 partitions${NC}"
  exit 1
fi

echo ""
echo -e "${BLUE}[6/8]${NC} Testing message flow (produce → consume)..."
echo '{"eventId":"test-1","eventType":"CLICK","timestamp":1234567890,"sessionId":"test-session","userId":"test-user"}' | \
  docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9056 --topic clickstream-events

echo ""
echo -e "${YELLOW}Consuming test message (timeout 5s)...${NC}"
CONSUMED=$(docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9056 \
  --topic clickstream-events \
  --from-beginning \
  --timeout-ms 5000 2>&1 || true)

if echo "$CONSUMED" | grep -q "test-1"; then
  echo "$CONSUMED" | grep "test-1"
  echo -e "${GREEN}✓ Message consumed successfully${NC}"
else
  echo -e "${RED}✗ ERROR: Failed to consume test message${NC}"
  exit 1
fi

echo ""
echo -e "${BLUE}[7/8]${NC} Testing MongoDB connection and write permissions..."
docker exec mongodb mongosh --eval "db.version()" clickstream_db
docker exec mongodb mongosh --eval "db.test.insertOne({test:1,timestamp:new Date()})" clickstream_db
echo -e "${GREEN}✓ MongoDB write test passed${NC}"

echo ""
echo -e "${BLUE}[8/8]${NC} Testing Kafka UI accessibility..."
if curl -f -s http://localhost:9050 > /dev/null; then
  echo -e "${GREEN}✓ Kafka UI is accessible${NC}"
else
  echo -e "${RED}✗ ERROR: Kafka UI not accessible at http://localhost:9050${NC}"
  exit 1
fi

echo ""
echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║                    ✓ Verification Complete                        ║"
echo "╚════════════════════════════════════════════════════════════════════╝"
echo ""
echo -e "${GREEN}✓${NC} Kafka broker:      ${YELLOW}localhost:9056${NC}"
echo -e "${GREEN}✓${NC} Kafka UI:          ${YELLOW}http://localhost:9050${NC}"
echo -e "${GREEN}✓${NC} MongoDB:           ${YELLOW}mongodb://localhost:9055/clickstream_db${NC}"
echo -e "${GREEN}✓${NC} Topic:             ${YELLOW}clickstream-events (6 partitions)${NC}"
echo ""
echo "Next steps:"
echo "  - Open Kafka UI at http://localhost:9050 to inspect topics and messages"
echo "  - Use MongoDB connection string for application development"
echo "  - Kafka bootstrap server: localhost:9056"
echo ""
