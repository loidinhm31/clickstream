# Clickstream Analytics - Quick Start Guide

## Prerequisites

Before starting, ensure you have the following installed:

- **Docker & Docker Compose** (for Kafka, MongoDB, Spark ETL)
- **Java 17+** (for Spring Boot services)
- **Maven 3.9+** (for building Java applications)
- **Node.js 18+** (for React frontend)
- **WSL** (if running on Windows)

Verify your environment:
```bash
make verify-setup
```

## Quick Start (3 Commands)

### 1. Initial Setup (First Time Only)
```bash
# Build all services
make build-all
```

This will:
- Build all Maven modules (shared-models, ingestion-api, realtime-analytics, raw-archiver, spark-etl)
- Install frontend npm dependencies
- Create production builds

### 2. Start Everything
```bash
# Start all services
make start-all
```

This will start:
- **Infrastructure** (Docker): Kafka, MongoDB, Kafka UI, Spark ETL
- **Application Services**: Ingestion API, Real-time Analytics, Raw Archiver
- **Frontend**: React application with Vite dev server

Wait ~30 seconds for all services to be healthy.

### 3. Access the Application

Once started, access:

| Service | URL | Description |
|---------|-----|-------------|
| **Frontend** | http://localhost:5173 | Main UI |
| **Ingestion API** | http://localhost:8081 | REST API for event ingestion |
| **Real-time Analytics** | ws://localhost:8082/ws/metrics | WebSocket metrics stream |
| **Raw Archiver Health** | http://localhost:8083/actuator/health | Health check |
| **Kafka UI** | http://localhost:8080 | Kafka topic browser |
| **MongoDB** | mongodb://localhost:27017/clickstream_db | Database |

## Common Commands

### Start/Stop
```bash
make start-all       # Start everything
make stop-all        # Stop everything
make restart-all     # Restart everything
make status          # Check service status
```

### Infrastructure Only
```bash
make start-infra     # Start Kafka, MongoDB, Kafka UI
make start-spark     # Build and start Spark ETL
make stop-infra      # Stop infrastructure
```

### Application Services
```bash
make start-ingestion-api      # Start Ingestion API only
make start-realtime-analytics # Start Real-time Analytics only
make start-raw-archiver       # Start Raw Archiver only
make start-frontend           # Start Frontend only
make stop-app-services        # Stop all app services
```

### Logs
```bash
make logs              # Tail all application logs
make logs-ingestion    # Ingestion API logs
make logs-realtime     # Real-time Analytics logs
make logs-archiver     # Raw Archiver logs
make logs-frontend     # Frontend logs
make logs-spark        # Spark ETL logs (Docker)
make logs-kafka        # Kafka logs (Docker)
```

### Build & Test
```bash
make build-all         # Build everything
make build-maven       # Build Maven modules only
make build-frontend    # Build frontend only
make test-all          # Run all tests
make test-maven        # Run Maven tests only
make test-frontend     # Run frontend tests only
```

### Cleanup
```bash
make clean-all         # Clean build artifacts + stop services
make clean-logs        # Clean log files only
make clean-maven       # Clean Maven artifacts
make clean-frontend    # Clean npm node_modules
make reset             # Full reset (stop all + clean all + remove Docker volumes)
```

## Development Workflow

### Full Development Setup
```bash
make dev
```
This runs `build-all` + `start-all` in one command.

### Quick Infrastructure Start
```bash
make quick-start
```
Starts only Kafka, MongoDB, Kafka UI, and Spark ETL. Use this if you want to manually run application services in your IDE.

### Checking Service Health

```bash
# Check overall status
make status

# Check specific service health
curl http://localhost:8081/actuator/health  # Ingestion API
curl http://localhost:8082/actuator/health  # Real-time Analytics
curl http://localhost:8083/actuator/health  # Raw Archiver
```

### Viewing Kafka Messages

1. Open Kafka UI: http://localhost:8080
2. Navigate to Topics → `clickstream-events`
3. Browse messages in real-time

### Viewing MongoDB Data

```bash
# Connect via mongosh
mongosh mongodb://localhost:27017/clickstream_db

# Query session aggregates
db.session_aggregates.find().pretty()

# Query page metrics
db.page_metrics.find().pretty()

# Query user journeys
db.user_journeys.find().pretty()
```

## Troubleshooting

### Services Not Starting

**Check Docker:**
```bash
docker compose ps
```

**Check application services:**
```bash
make status
```

**View logs for errors:**
```bash
make logs
```

### Port Conflicts

If ports are already in use, stop conflicting services or modify ports in `Makefile`:
- `INGESTION_API_PORT=8081`
- `REALTIME_ANALYTICS_PORT=8082`
- `RAW_ARCHIVER_PORT=8083`
- `FRONTEND_PORT=5173`

### Maven Build Issues

If you encounter dependency download errors:
```bash
# Check Artifactory/Maven settings
cat .mvn/settings.xml

# Try building with explicit settings
mvn clean install --settings .mvn/settings.xml
```

### Frontend Not Starting

```bash
# Reinstall dependencies
cd frontend
npm install
npm run dev
```

### Spark ETL Not Working

```bash
# Rebuild Spark ETL
make build-spark-etl

# Restart Spark container
docker compose restart spark-etl

# Check logs
make logs-spark
```

### Full Reset

If everything is broken, do a full reset:
```bash
make reset
make build-all
make start-all
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         FRONTEND (React)                        │
│                    http://localhost:5173                        │
└────────┬────────────────────────────────────────────────┬───────┘
         │ HTTP/REST                            WebSocket │
         ▼                                                 ▼
┌────────────────────┐                      ┌──────────────────────┐
│  Ingestion API     │                      │ Real-time Analytics  │
│  (Spring Boot)     │                      │ (Arrow + WebSocket)  │
│  :8081             │                      │ :8082                │
└────────┬───────────┘                      └──────────┬───────────┘
         │ Kafka Produce                               │ Kafka Consume
         │                                             │
         ▼                                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   KAFKA (Event Stream)                          │
│                   Topic: clickstream-events                     │
│                   localhost:9092                                │
└────┬──────────────────────────┬──────────────────────────┬──────┘
     │                          │                          │
     │ Consumer                 │ Consumer                 │ Consumer
     ▼                          ▼                          ▼
┌─────────────┐      ┌───────────────────┐     ┌─────────────────┐
│ Spark ETL   │      │ Real-time Analytics│     │ Raw Archiver    │
│ (Docker)    │      │ Engine             │     │ (Spring Boot)   │
│             │      │                    │     │ :8083           │
└──────┬──────┘      └──────┬────────────┘     └────────┬────────┘
       │                    │                           │
       │ Write              │ In-Memory                 │ Write
       │                    │ (Arrow)                   │ Parquet
       ▼                    │                           ▼
┌─────────────┐             │                  ┌─────────────────┐
│  MongoDB    │             │                  │  Data Lake      │
│  :27017     │◄────────────┘                  │  (Filesystem)   │
│             │     Query for                  │  Parquet Files  │
└─────────────┘     Historical Data            └─────────────────┘
```

## Service Descriptions

### Infrastructure (Docker)

- **Kafka**: Event streaming platform, stores clickstream events
- **MongoDB**: Document database for aggregated session/page metrics
- **Kafka UI**: Web interface for Kafka topic inspection
- **Spark ETL**: Structured streaming job for aggregations (runs in Docker)

### Application Services (Spring Boot)

- **Ingestion API**: Receives events from frontend, validates, publishes to Kafka
- **Real-time Analytics**: Consumes events, computes real-time metrics (Arrow), pushes via WebSocket
- **Raw Archiver**: Archives raw events to Parquet files for data lake

### Frontend (React + Vite)

- **React 19**: UI framework
- **Apache Arrow**: Efficient columnar data consumption
- **Recharts**: Real-time metrics visualization
- **React Router**: Navigation
- **TanStack Query**: Data fetching and caching

## Next Steps

1. **Test the pipeline**: Open http://localhost:5173 and interact with the UI
2. **Monitor Kafka**: Open http://localhost:8080 and watch events flow
3. **Check MongoDB**: Query aggregated data with `mongosh`
4. **View Parquet files**: Check `data-lake/raw-events/` directory
5. **Customize**: Modify services and restart with `make restart-all`

## Help

For all available commands:
```bash
make help
```

For issues or questions, check the project documentation in `docs/` or review service-specific READMEs:
- `ingestion-api/README.md`
- `realtime-analytics/README.md`
- `raw-archiver/README.md`
- `spark-etl/README.md`
- `frontend/README.md`
