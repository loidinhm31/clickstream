# Research Report: Running a Clickstream Analytics Stack

## Executive Summary
Running a full-stack clickstream analytics system (Spring Boot, Kafka, Spark, React) requires a carefully orchestrated "Infrastructure-First" approach. The primary challenge in local development is managing inter-service dependencies and ensuring environment parity between the host machine and Docker containers. This report outlines the optimal startup sequence, categorizes components for deployment efficiency, and highlights critical pitfalls to avoid in local and production-like setups.

---

## Optimal Startup Order
The stack should be initialized in a "Bottom-Up" sequence, ensuring data sinks and streams are ready before producers begin sending data.

1.  **Phase 1: Foundation (Infrastructure)**
    - **Components**: Zookeeper/KRaft, Kafka Broker, MongoDB/PostgreSQL.
    - **Reasoning**: These are the "dumb" storage and transport layers that other services depend on.
    - **Health Check**: Ensure the Kafka broker is accepting connections and the database is pingable.

2.  **Phase 2: Topic & Schema Setup**
    - **Components**: Topic initialization scripts, Schema Registry.
    - **Reasoning**: Consumers and Producers will fail if topics don't exist or schemas are undefined.
    - **Local command**: `make start-infra` runs `docker compose up kafka-init` after the broker is available so `clickstream-events` exists before producers and consumers start.

3.  **Phase 3: Sinks & Processing (Consumers)**
    - **Components**: Spark ETL, Real-time Analytics, Raw Archiver.
    - **Reasoning**: Starting consumers before producers prevents data build-up in Kafka and allows verification of the pipeline from the "end" first.
    - **Constraint**: Must wait for Phase 1 & 2 to be healthy.
    - **JDK compatibility**: The Real-time Analytics service uses Apache Arrow and needs `--add-opens=java.base/java.nio=ALL-UNNAMED` on modern JDKs.

4.  **Phase 4: Ingestion API (Producers)**
    - **Components**: Spring Boot Ingestion API.
    - **Reasoning**: The API requires Kafka to be ready to accept events.
    - **Best Practice**: Implement exponential backoff for the initial Kafka connection.

5.  **Phase 5: Presentation (Frontend)**
    - **Components**: React Application (Vite/Webpack).
    - **Reasoning**: The frontend depends on the Ingestion API for event tracking and the Analytics API for displaying data.
    - **E2E default**: Vite runs on port `9059`; configure proxy targets for Docker service URLs when the browser runs inside the Docker network.

---

## Component Categorization

### Docker-First (Run in Docker)
These components benefit from the isolation and configuration consistency of containerization.
- **Kafka & Zookeeper**: Complex networking and state management are handled via Docker Compose.
- **Databases (MongoDB)**: Ensures identical versions and storage configurations across the team.
- **Spark ETL**: Local Spark/Hadoop installation is notoriously difficult on Windows/WSL. Running Spark in Docker avoids "WinUtils" and environment variable hell.

### Dev-Friendly (Run Locally on Host)
These components are under active development and require fast feedback loops.
- **React Frontend**: Vite's Hot Module Replacement (HMR) is significantly faster when running natively. Docker volume syncing can introduce latency and file-watching issues.
- **Ingestion API**: Running natively in an IDE (IntelliJ/VS Code) allows for seamless debugging, profiling, and faster restarts than rebuilding Docker images.
- **Real-time Analytics**: Similar to the API, this often requires frequent logic tweaks and benefit from IDE-based development.

---

## Common Pitfalls in Local Setup

### 1. The Kafka "Advertised Listener" Trap
- **Issue**: A Spring Boot app running on the **host** uses `localhost:9056`, but an app in a **container** needs `kafka:9094`. 
- **Solution**: Configure multiple listeners in `docker-compose.yml` (e.g., `HOST` and `DOCKER`).

### 2. "Dumb" Dependency Management
- **Issue**: Using `depends_on` in Docker without health checks. The container starts, but the service inside (e.g., Kafka) takes 15 seconds to be "ready."
- **Solution**: Use `condition: service_healthy` and robust `healthcheck` commands in your Compose file.

### 3. Spark Checkpoint Corruption
- **Issue**: Local Spark jobs often use temporary directories for checkpoints. If the directory is deleted or permissions change between runs, the streaming job will crash.
- **Solution**: Mount a persistent volume for `/tmp/spark-checkpoints` or use a dedicated local directory with stable permissions.

### 4. Memory Exhaustion
- **Issue**: Running Kafka (2GB), Spark (2GB), MongoDB (1GB), and multiple Spring Boot apps (512MB each) simultaneously can easily exceed 8GB-16GB RAM.
- **Solution**: Set explicit `mem_limit` in Docker and use `-Xmx` flags for local Java processes.

### 5. Time Synchronization
- **Issue**: If using WSL2, the clock can occasionally drift from the host. Clickstream windowing (e.g., 30-min sessions) will produce incorrect results.
- **Solution**: Regularly sync the WSL clock or use NTP-synced containers.

---

## Implementation Recommendations

### Recommended Setup Script (Makefile)
```makefile
start:
	docker-compose up -d kafka mongodb
	./scripts/wait-for-it.sh localhost:9056 --timeout=30
	mvn spring-boot:run -pl ingestion-api &
	cd frontend && npm run dev
```

### Critical Health Checks
Ensure every service has a `/health` or `/actuator/health` endpoint. The Ingestion API should report as "DOWN" if it cannot connect to Kafka.

## Unresolved Questions
- **Scaling**: How does the startup order change when moving to a managed service like Confluent Cloud?
- **Data Integrity**: Best practices for idempotent processing if the Ingestion API retries a batch?
