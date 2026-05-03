# Phase 01: Infrastructure

Setup and initialization of the core message broker and database.

## 1. Apache Kafka (KRaft Mode)

We use Kafka 3.7.0 in KRaft mode to avoid Zookeeper dependency.

**Configuration Highlights:**
- **External Port**: `9056` (Host access)
- **Internal Port**: `9094` (Docker network)
- **Topic**: `clickstream-events` (6 partitions, RF=1 for dev)

**Commands:**
```bash
# Start Kafka and initialization service
docker compose up -d kafka kafka-init
```

**Verification:**
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9094 --list
# Expected output: clickstream-events
```

## 2. MongoDB

Primary document store for session aggregates and page metrics.

**Configuration Highlights:**
- **Port**: `9055`
- **Database**: `clickstream_db`

**Commands:**
```bash
# Start MongoDB
docker compose up -d mongodb
```

**Verification:**
```bash
docker exec mongodb mongosh --eval "db.adminCommand('ping')"
```

## 3. Kafka UI

Visual interface for monitoring topics and consumer groups.

**Access**: `http://localhost:9050`

**Commands:**
```bash
docker compose up -d kafka-ui
```

## Summary Checklist
- [ ] Kafka container running and healthy
- [ ] `clickstream-events` topic created
- [ ] MongoDB container running and healthy
- [ ] Kafka UI accessible
