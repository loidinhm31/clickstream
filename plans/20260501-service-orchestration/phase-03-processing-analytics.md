# Phase 03: Processing & Analytics

Starting the downstream consumers that process Kafka events.

## 1. Spark ETL (Dockerized)

Processes raw events into session-level aggregates in MongoDB.

**Commands:**
```bash
# Start Spark ETL via Docker Compose
docker compose up -d spark-etl
```

**Verification:**
```bash
docker logs -f spark-etl
# Look for "Streaming query started" and MongoDB upsert logs
```

## 2. Real-time Analytics

Provides sub-second metrics via WebSockets and Apache Arrow.

**Build and Run:**
```bash
cd realtime-analytics
mvn clean package -DskipTests
java -jar target/realtime-analytics-0.0.1-SNAPSHOT.jar
```

**Verification:**
```bash
curl http://localhost:9052/api/realtime/health
```

## 3. Raw Archiver

Persists all raw events to Parquet files for long-term storage.

**Build and Run:**
```bash
cd raw-archiver
mvn clean package -DskipTests
java -jar target/raw-archiver-0.0.1-SNAPSHOT.jar
```

**Verification:**
```bash
curl http://localhost:9053/actuator/health
ls -R data-lake/
```

## Summary Checklist
- [ ] Spark ETL running in Docker and writing to MongoDB
- [ ] Real-time Analytics accessible on port 9052
- [ ] Raw Archiver running and creating Parquet files in `data-lake/`
- [ ] All consumers visible in Kafka UI
