# Phase 01 — Dev Environment (Docker Compose)

## Context
- Parent: [plan.md](./plan.md)
- Research: [researcher-01-report.md](./research/researcher-01-report.md)

## Overview
- **Priority:** P0 (blocker for all other phases)
- **Status:** Done ✓
- **Completed:** 2026-04-18
- **Effort:** 3h
- **Tests:** 23/23 passed
- **Code Review Score:** 9/10
- **Description:** Docker Compose setup with Kafka (KRaft), MongoDB, Kafka UI, plus init scripts for topic creation.

## Key Insights
- KRaft mode eliminates ZooKeeper — single container for Kafka in combined mode
- Two listeners required: HOST for apps on host machine, DOCKER for inter-container
- `apache/kafka:latest` (JVM) for dev; `apache/kafka-native` is faster but lacks CLI tools
- Kafbat UI (`kafbat/kafka-ui`) is actively maintained, lightweight

## Requirements
- Single Kafka broker with `clickstream-events` topic auto-created (6 partitions)
- MongoDB with persistent volume
- Kafka UI accessible at localhost:8080
- All services on shared Docker network
- Spring Boot, Spark, Real-time Service connect from host machine

## Architecture

```yaml
# docker-compose.yml
services:
  kafka:
    image: apache/kafka:latest
    container_name: kafka
    ports:
      - "9092:9092"    # Host access
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: CONTROLLER://kafka:9093,HOST://0.0.0.0:9092,DOCKER://0.0.0.0:9094
      KAFKA_ADVERTISED_LISTENERS: HOST://localhost:9092,DOCKER://kafka:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,HOST:PLAINTEXT,DOCKER:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: DOCKER
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_LOG_RETENTION_HOURS: 168
    volumes:
      - kafka-data:/var/lib/kafka/data
    networks:
      - clickstream-net

  kafka-init:
    image: apache/kafka:latest
    container_name: kafka-init
    depends_on:
      - kafka
    entrypoint: ["/bin/sh", "-c"]
    command: |
      "
      echo 'Waiting for Kafka...'
      sleep 10
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9094 --create --if-not-exists \
        --topic clickstream-events --partitions 6 --replication-factor 1
      echo 'Topic created.'
      "
    networks:
      - clickstream-net

  kafka-ui:
    image: kafbat/kafka-ui:main
    container_name: kafka-ui
    ports:
      - "8080:8080"
    environment:
      DYNAMIC_CONFIG_ENABLED: "true"
      KAFKA_CLUSTERS_0_NAME: clickstream-local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9094
    depends_on:
      - kafka
    networks:
      - clickstream-net

  mongodb:
    image: mongo:7
    container_name: mongodb
    ports:
      - "27017:27017"
    environment:
      MONGO_INITDB_DATABASE: clickstream_db
    volumes:
      - mongo-data:/data/db
    networks:
      - clickstream-net

volumes:
  kafka-data:
  mongo-data:

networks:
  clickstream-net:
    driver: bridge
```

## Related Code Files
- **Create:** `docker-compose.yml` (project root)
- **Create:** `scripts/verify-setup.sh` (verification script)

## Implementation Steps

1. Create `docker-compose.yml` in project root with config above
2. Create `scripts/verify-setup.sh`:
   ```bash
   #!/bin/bash
   echo "=== Starting services ==="
   docker compose up -d
   echo "=== Waiting 15s for Kafka init ==="
   sleep 15
   echo "=== Listing topics ==="
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
   echo "=== Producing test message ==="
   echo '{"eventId":"test-1","eventType":"CLICK","timestamp":1234567890}' | \
     docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic clickstream-events
   echo "=== Consuming test message ==="
   docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic clickstream-events --from-beginning --timeout-ms 5000
   echo "=== Open Kafka UI at http://localhost:8080 ==="
   echo "=== MongoDB at mongodb://localhost:27017/clickstream_db ==="
   ```
3. Run verification script, confirm topic visible in Kafka UI at localhost:8080

## Todo
- [x] Create docker-compose.yml
- [x] Create verification script
- [x] Test full lifecycle: produce → consume → inspect in UI
- [x] Document connection strings for all services

## Success Criteria
- ✓ `docker compose up -d` starts all containers without errors
- ✓ `clickstream-events` topic exists with 6 partitions
- ✓ Test message round-trips (produce → consume)
- ✓ Kafka UI shows topic, partitions, messages at localhost:8080
- ✓ MongoDB accepts connections at localhost:27017

## Risk Assessment
- **Port conflicts:** 9092, 8080, 27017 commonly used. Document alternative port mapping.
- **Kafka startup race:** init container may run before Kafka is ready. Sleep 10s mitigates; health check better for prod.

## Security Considerations
- Dev only — no auth on Kafka or MongoDB. Prod needs SASL/SSL and MongoDB auth.

## Next Steps
- Phase 2: Define event schema and Kafka configuration details
- All service phases use connection string `localhost:9092` (Kafka) and `localhost:27017` (MongoDB)
