# Clickstream Analytics Application

End-to-end clickstream analytics system capturing user micro-events from a React frontend, ingesting via Spring Boot into Kafka, then processing through three independent consumer groups: Spark ETL (→ MongoDB), Real-time Analytics (Arrow in-memory → Arrow Flight → frontend), and Raw Archiver (→ Parquet data lake).

## Architecture

```
React Frontend (micro-events)
    ↓
Spring Boot Ingestion API
    ↓
Apache Kafka (clickstream-events topic)
    ↓
    ├─→ Spark ETL → MongoDB (aggregated sessions)
    ├─→ Real-time Analytics (Arrow in-memory) → WebSocket → Frontend
    └─→ Raw Archiver → Parquet Data Lake
```

## Development Environment

### Prerequisites

- Docker & Docker Compose (in WSL for Windows users)
- Java 17+ (for Spring Boot)
- Node.js 18+ (for React frontend)
- Maven or Gradle

### Quick Start

1. **Start infrastructure services:**

```bash
# Start Kafka, MongoDB, and Kafka UI
docker compose up -d

# Verify setup (runs automated tests)
bash scripts/verify-setup.sh
```

2. **Access services:**

- **Kafka UI:** http://localhost:8080
- **Kafka Broker:** localhost:9092
- **MongoDB:** mongodb://localhost:27017/clickstream_db

### Services

| Service | Port | Description |
|---------|------|-------------|
| Apache Kafka | 9092 | Message broker (KRaft mode) |
| Kafbat UI | 8080 | Web UI for Kafka inspection |
| MongoDB | 27017 | Document database for aggregated data |

### Kafka Topics

- **clickstream-events** (6 partitions)
  - Partition key: `sessionId`
  - Retention: 24 hours (1 day)
  - Consumer groups: spark-etl, realtime-analytics, raw-archiver

## Project Structure

```
clickstream/
├── docker-compose.yml          # Infrastructure setup
├── scripts/
│   └── verify-setup.sh         # Automated verification
├── plans/                      # Implementation plans
│   └── 20260418-init-clickstream/
│       ├── plan.md             # Master plan
│       └── phase-*.md          # Detailed phase plans
└── [TBD: service directories]
    ├── ingestion-api/          # Spring Boot REST API
    ├── spark-etl/              # Spark Structured Streaming
    ├── realtime-analytics/     # Arrow in-memory analytics
    ├── raw-archiver/           # Parquet writer
    └── frontend/               # React application
```

## Development Workflow

1. **Phase 01:** Dev environment (Docker Compose) ✓
2. **Phase 02:** Kafka topic design & event schema
3. **Phase 03:** Spring Boot ingestion API
4. **Phase 04:** Spark ETL pipeline
5. **Phase 05:** Real-time analytics service (Arrow)
6. **Phase 06:** Raw event archiver
7. **Phase 07:** React frontend (Atomic Design)

See [plans/20260418-init-clickstream/plan.md](plans/20260418-init-clickstream/plan.md) for complete roadmap.

## Key Technologies

- **Apache Kafka** (KRaft mode) - Event streaming platform
- **Spring Boot** - REST API framework
- **Apache Spark** - Distributed data processing
- **Apache Arrow** - In-memory columnar format
- **MongoDB** - Document database
- **React** - Frontend framework
- **Parquet** - Columnar storage format

## Development Environment Details

### Why KRaft Mode (No ZooKeeper)

We use Apache Kafka in KRaft (Kraft Consensus) mode instead of the traditional ZooKeeper-based architecture for these reasons:

- **Simplified Operations:** Single control plane removes ZooKeeper coordination complexity
- **Reduced Resource Overhead:** No separate ZooKeeper cluster needed in development
- **Future-Ready:** KRaft is Kafka's recommended mode for versions 4.0+
- **Faster Boot:** Fewer services to initialize during startup
- **Operational Consistency:** Same metadata architecture as production deployments

In `docker-compose.yml`, note the environment variables:
- `KAFKA_PROCESS_ROLES: broker,controller` - Single node acts as both broker and controller
- `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093` - Single voter quorum for dev

### Resource Limits for Development

The Docker Compose services are configured with resource limits for safe local development:

| Service | Memory | CPU | Rationale |
|---------|--------|-----|-----------|
| Kafka | 2 GB | 2 CPU | Sufficient for message ingestion and buffering |
| MongoDB | 1 GB | 1 CPU | Document storage and indexing |
| Kafka UI | 1 GB | 1 CPU | Web interface for cluster inspection |
| kafka-init | 512 MB | 0.5 CPU | One-time topic initialization task |

These limits prevent runaway containers from consuming all system resources. For higher-throughput testing, adjust `mem_limit` and `cpus` in `docker-compose.yml`.

### Health Check Configuration

Each primary service includes health checks to ensure readiness before dependent services start:

- **Kafka:** Validates broker API versions (10s interval, 30s startup grace)
- **MongoDB:** Runs `db.adminCommand('ping')` (10s interval, 20s startup grace)
- **Kafka UI:** HTTP health endpoint check (10s interval, 20s startup grace)

The `kafka-init` service depends on Kafka's `service_healthy` condition, ensuring the broker is ready before creating topics.

## Testing

```bash
# Produce test event
echo '{"eventId":"test-1","eventType":"CLICK","timestamp":1234567890}' | \
  docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic clickstream-events

# Consume events
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic clickstream-events \
  --from-beginning
```

## Manage Services

```bash
# Start all services
docker compose up -d

# View logs
docker compose logs -f

# Stop all services
docker compose down

# Stop and remove volumes (clean slate / reset environment)
docker compose down -v
```

## Troubleshooting

### Port Conflicts

If you encounter "port already in use" errors:

```bash
# Check which process is using the port
# On Windows/WSL:
netstat -ano | findstr :9092
netstat -ano | findstr :8080
netstat -ano | findstr :27017

# On Linux/macOS:
lsof -i :9092
lsof -i :8080
lsof -i :27017

# Option 1: Stop the conflicting service
# Option 2: Change ports in docker-compose.yml
# Example: "9093:9092" instead of "9092:9092"
```

### Kafka Not Ready

If services fail to start or topics aren't created:

```bash
# Check Kafka logs
docker logs kafka

# Verify Kafka is responding
docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# Restart with clean state
docker compose down -v
docker compose up -d
```

### MongoDB Connection Refused

If MongoDB connection fails:

```bash
# Check MongoDB logs
docker logs mongodb

# Test connection manually
docker exec mongodb mongosh --eval "db.version()" clickstream_db

# Verify port is listening
docker ps | grep mongodb
```

### WSL Docker Bridge Networking

If services can't communicate or localhost:9092 doesn't work from Windows:

```bash
# Verify Docker is running in WSL2 (not WSL1)
wsl -l -v

# Check Docker daemon is running
wsl docker ps

# Access services from WSL using localhost
# Access services from Windows using localhost (Docker Desktop required)

# Alternative: Use host.docker.internal instead of localhost in connection strings
```

### Slow Startup / Timing Issues

If kafka-init fails or verification times out:

```bash
# Increase wait time in verify-setup.sh 
# Health checks will ensure services are ready

# Check container health status
docker compose ps

# Wait for all services to be healthy
until docker inspect --format='{{.State.Health.Status}}' kafka | grep -q healthy; do sleep 2; done
```

### Images Not Pulling

If Docker can't pull images:

```bash
# Pre-pull images
docker compose pull

# Check Docker Hub connectivity
docker pull apache/kafka:3.7.0

# Use mirror if Docker Hub is blocked
# (edit docker-compose.yml with alternative registry)
```

## Next Steps

- [ ] Define event schema (Phase 02)
- [ ] Implement Spring Boot ingestion API (Phase 03)
- [ ] Build Spark ETL pipeline (Phase 04)
- [ ] Develop real-time analytics service (Phase 05)
- [ ] Create raw event archiver (Phase 06)
- [ ] Build React frontend (Phase 07)

## License

[TBD]

## Contributing

[TBD]
