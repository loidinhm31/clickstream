# Deployment Guide

Production deployment, scaling, monitoring, and operational considerations.

## Pre-Deployment Checklist

### Code & Configuration

- [ ] All tests passing: `mvn clean verify`
- [ ] Security review completed (dependencies, OWASP Top 10)
- [ ] Configuration for target environment created (prod profile)
- [ ] Sensitive data removed (no secrets in code)
- [ ] Documentation updated

### Database & Indexes

- [ ] MongoDB connection pool tuned for load
- [ ] Indexes created on all query fields
- [ ] Replication factor set to 3 (production minimum)
- [ ] Backup strategy defined

### Kafka & Streaming

- [ ] Topic replication factor set to 3
- [ ] Producer `acks` configuration set to `all` (durability)
- [ ] Consumer group lag monitoring configured
- [ ] Capacity planning: partitions >= number of concurrent producer threads

### Monitoring & Observability

- [ ] Application metrics collection enabled (Micrometer)
- [ ] Logging aggregation configured (e.g., ELK stack)
- [ ] Health check endpoint monitored
- [ ] Alert rules for high error rate, latency SLA breach

---

## Docker Deployment

### Build Docker Image

```dockerfile
# Dockerfile
FROM eclipse-temurin:17-jre-alpine
COPY target/ingestion-api-1.0.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
```

**Build:**
```bash
mvn clean package
docker build -t clickstream/ingestion-api:1.0.0 .
```

### Docker Compose (Single-Node Development)

```yaml
version: '3.9'
services:
  ingestion-api:
    image: clickstream/ingestion-api:1.0.0
    ports:
      - "8081:8081"
    environment:
      SPRING_SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      SPRING_DATA_MONGODB_URI: mongodb://mongo:27017/clickstream_db
      CLICKSTREAM_CORS_ALLOWED_ORIGINS: "http://localhost:3000"
    depends_on:
      - kafka
      - mongo
    networks:
      - clickstream-net
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/actuator/health"]
      interval: 30s
      timeout: 5s
      retries: 3

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    depends_on:
      - zookeeper
    networks:
      - clickstream-net

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    networks:
      - clickstream-net

  mongo:
    image: mongo:7.0
    ports:
      - "27017:27017"
    environment:
      MONGO_INITDB_DATABASE: clickstream_db
    volumes:
      - mongo-data:/data/db
    networks:
      - clickstream-net

volumes:
  mongo-data:

networks:
  clickstream-net:
    driver: bridge
```

**Deploy:**
```bash
docker compose up -d ingestion-api
docker logs -f ingestion-api
```

---

## Kubernetes Deployment

### Deployment Manifest

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ingestion-api
  namespace: clickstream
spec:
  replicas: 3
  selector:
    matchLabels:
      app: ingestion-api
  template:
    metadata:
      labels:
        app: ingestion-api
        version: "1.0.0"
    spec:
      containers:
      - name: ingestion-api
        image: clickstream/ingestion-api:1.0.0
        ports:
        - containerPort: 8081
          name: http
        env:
        - name: SPRING_PROFILES_ACTIVE
          value: "prod"
        - name: SPRING_SPRING_KAFKA_BOOTSTRAP_SERVERS
          value: "kafka-broker-0.kafka-broker-headless.kafka:9092,kafka-broker-1.kafka-broker-headless.kafka:9092,kafka-broker-2.kafka-broker-headless.kafka:9092"
        - name: SPRING_DATA_MONGODB_URI
          valueFrom:
            secretKeyRef:
              name: mongodb-secret
              key: connection-string
        - name: CLICKSTREAM_CORS_ALLOWED_ORIGINS
          value: "https://analytics.example.com"
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /actuator/health
            port: 8081
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /actuator/health/readiness
            port: 8081
          initialDelaySeconds: 10
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 2

---
apiVersion: v1
kind: Service
metadata:
  name: ingestion-api
  namespace: clickstream
spec:
  type: LoadBalancer
  ports:
  - port: 80
    targetPort: 8081
    protocol: TCP
    name: http
  selector:
    app: ingestion-api

---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: ingestion-api-hpa
  namespace: clickstream
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: ingestion-api
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

**Deploy:**
```bash
kubectl apply -f ingestion-api-deployment.yaml

# Monitor
kubectl get pods -n clickstream
kubectl logs -f -n clickstream deployment/ingestion-api
```

---

## Configuration for Production

### application-prod.yml

```yaml
spring:
  application:
    name: clickstream-ingestion-api
  
  kafka:
    bootstrap-servers: kafka-prod-1:9092,kafka-prod-2:9092,kafka-prod-3:9092
    producer:
      acks: all                     # Wait for all replicas (durability)
      compression-type: snappy      # Better compression ratio
      batch-size: 65536             # 64KB for better throughput
      linger-ms: 50                 # Batch window 50ms
      retries: 10                   # Retry failed sends
      retry-backoff-ms: 1000        # Back off 1s between retries
      max-in-flight-requests-per-connection: 5  # Preserve ordering
  
  data:
    mongodb:
      uri: mongodb+srv://${DB_USER}:${DB_PASSWORD}@cluster-prod.mongodb.net/clickstream_db?retryWrites=true&w=majority&ssl=true
      auto-index-creation: false

spring.data.mongodb:
  max-pool-size: 200
  min-pool-size: 50
  max-connection-idle-time: 120000   # 2 minutes
  max-connection-life-time: 1800000  # 30 minutes
  
server:
  port: 8081
  tomcat:
    threads:
      max: 200                       # HTTP thread pool
      min-spare: 20
    max-connections: 10000
  compression:
    enabled: true
    min-response-size: 1024          # Compress responses > 1KB

management:
  endpoints:
    web:
      exposure:
        include: health,metrics,prometheus  # For monitoring
  metrics:
    export:
      prometheus:
        enabled: true

logging:
  level:
    root: WARN
    com.clickstream: INFO
    org.springframework.kafka: INFO
  pattern:
    console: "%d{yyyy-MM-dd'T'HH:mm:ss.SSS'Z'} [%thread] %-5level %logger{36} - %msg%n"

clickstream:
  cors:
    allowed-origins: "https://analytics.example.com,https://api.example.com"
  kafka:
    topic: clickstream-events-prod
  batch:
    max-size: 100
```

### Environment Variables

```bash
# Database credentials (from secrets manager)
export DB_USER=prod_user
export DB_PASSWORD=<secure-password>

# Kafka broker addresses
export KAFKA_BOOTSTRAP_SERVERS="kafka-1:9092,kafka-2:9092,kafka-3:9092"

# Application configuration
export SPRING_PROFILES_ACTIVE=prod
export CLICKSTREAM_CORS_ALLOWED_ORIGINS="https://analytics.example.com"

# Monitoring
export MANAGEMENT_METRICS_EXPORT_PROMETHEUS_ENABLED=true
```

---

## Database Setup

### MongoDB Production Configuration

1. **Create Replica Set** (if not using Atlas):
```bash
# Create replica set for durability
mongosh --eval "
  rs.initiate({
    _id: 'rs0',
    members: [
      {_id: 0, host: 'mongo-1:27017'},
      {_id: 1, host: 'mongo-2:27017'},
      {_id: 2, host: 'mongo-3:27017'}
    ]
  })
"
```

2. **Create Indexes:**
```javascript
use clickstream_db

// Critical indexes for analytics queries
db.sessions.createIndex({ userId: 1, startTime: -1 }, { background: true })
db.sessions.createIndex({ sessionId: 1 }, { unique: true })
db.sessions.createIndex({ startTime: -1 })

db.page_metrics.createIndex({ pageUrl: 1 }, { background: true })
db.page_metrics.createIndex({ lastUpdated: -1 })

db.user_journeys.createIndex({ userId: 1 }, { background: true })
db.user_journeys.createIndex({ sessionId: 1 })

// TTL index: auto-delete documents older than 90 days
db.sessions.createIndex({ startTime: 1 }, { expireAfterSeconds: 7776000 })
```

3. **Configure Connection Security:**
```yaml
spring.data.mongodb:
  uri: mongodb+srv://user:password@cluster.mongodb.net/clickstream_db?authSource=admin&ssl=true&retryWrites=true&w=majority
```

### Backup Strategy

**MongoDB Atlas (recommended):**
- Continuous backups enabled
- Point-in-time restore: 7 days
- Daily snapshots to S3

**Self-Managed:**
```bash
# Daily backup script
#!/bin/bash
mongodump --uri "mongodb://user:pass@mongo:27017/clickstream_db" \
          --out /backups/mongodb-$(date +%Y%m%d)
          
# Compress
tar -czf /backups/mongodb-$(date +%Y%m%d).tar.gz \
    /backups/mongodb-$(date +%Y%m%d)
    
# Upload to S3
aws s3 cp /backups/mongodb-$(date +%Y%m%d).tar.gz \
    s3://clickstream-backups/mongodb/
```

---

## Kafka Setup

### Topic Replication for Production

```bash
# Create topic with replication factor 3
kafka-topics.sh --create \
  --bootstrap-server kafka-1:9092 \
  --topic clickstream-events \
  --partitions 12 \
  --replication-factor 3 \
  --retention-ms 604800000  # 7 days

# Verify
kafka-topics.sh --describe \
  --bootstrap-server kafka-1:9092 \
  --topic clickstream-events
```

### Producer Tuning

| Setting | Dev | Prod | Reason |
|---------|-----|------|--------|
| `acks` | 1 | all | Durability: wait for all replicas |
| `min-insync-replicas` | - | 2 | At least 2 replicas acknowledge |
| `batch-size` | 16KB | 64KB | Higher throughput |
| `linger-ms` | 5 | 50 | More batching time |
| `compression-type` | lz4 | snappy | Better ratio-speed tradeoff |

---

## Monitoring & Observability

### Metrics Collection (Micrometer + Prometheus)

**Endpoints exposed:**
```
GET /actuator/metrics                    # All available metrics
GET /actuator/metrics/jvm.memory         # JVM memory
GET /actuator/metrics/http.server.requests  # HTTP endpoint latency
GET /actuator/prometheus                # Prometheus scrape format
```

**Prometheus scrape config:**
```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'ingestion-api'
    static_configs:
      - targets: ['localhost:8081']
    metrics_path: '/actuator/prometheus'
```

### Key Metrics to Monitor

| Metric | Alert Threshold | Unit |
|--------|-----------------|------|
| `http.server.requests` (p99) | > 50ms | milliseconds |
| `http_requests_total` (5xx) | > 1% | percentage |
| `jvm.memory.used` | > 80% | percentage |
| `process.cpu.usage` | > 80% | percentage |
| `kafka.producer.records.lag` | > 1000 | messages |

### Health Check

```bash
# Application readiness
curl http://localhost:8081/actuator/health/readiness

# Response: 200 OK
# {"status": "UP"}
```

### Logging Aggregation (ELK Stack)

**Fluent Bit Configuration:**
```ini
[INPUT]
    Name              tail
    Path              /var/log/containers/ingestion-api-*.log
    Parser            docker
    Tag               ingestion-api.*

[FILTER]
    Name              kubernetes
    Match             ingestion-api.*

[OUTPUT]
    Name              es
    Match             ingestion-api.*
    Host              elasticsearch.monitoring
    Port              9200
    Logstash_Format   On
    Logstash_Prefix   clickstream-ingestion
    Type              _doc
```

---

## Scaling Strategies

### Horizontal Scaling (Multiple Instances)

**Load Balancer Configuration (Nginx):**
```nginx
upstream ingestion_api {
    least_conn;
    server api-1:8081 weight=1;
    server api-2:8081 weight=1;
    server api-3:8081 weight=1;
}

server {
    listen 80;
    location /api/events {
        proxy_pass http://ingestion_api;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### Vertical Scaling (Larger Instance)

**JVM Tuning for 16GB server:**
```bash
java -Xms8G -Xmx8G \
     -XX:+UseG1GC \
     -XX:MaxGCPauseMillis=200 \
     -XX:+ParallelRefProcEnabled \
     -jar ingestion-api-1.0.0-SNAPSHOT.jar
```

### Rate Limiting for DDoS Protection

**NGINX Rate Limiting:**
```nginx
limit_req_zone $binary_remote_addr zone=api_limit:10m rate=10r/s;
limit_req_zone $server_name zone=server_limit:10m rate=100r/s;

server {
    location /api/events {
        limit_req zone=api_limit burst=20 nodelay;
        limit_req zone=server_limit burst=100 nodelay;
        proxy_pass http://ingestion_api;
    }
}
```

---

## Security Considerations

### Secrets Management

**Never commit secrets to Git:**
```yaml
# ❌ DON'T
spring:
  data:
    mongodb:
      uri: mongodb+srv://user:MyPassword123@cluster.mongodb.net

# ✅ DO
spring:
  data:
    mongodb:
      uri: ${DB_CONNECTION_STRING}  # From environment or vault
```

**Use HashiCorp Vault:**
```bash
vault kv put secret/clickstream/db \
  connection-string=mongodb+srv://user:pass@cluster.mongodb.net

# Application reads from vault
export DB_CONNECTION_STRING=$(vault kv get -field=connection-string secret/clickstream/db)
```

### Network Security

- [ ] Use HTTPS/TLS for all connections (Kafka, MongoDB)
- [ ] Enable CORS with specific allowed origins
- [ ] Rate limit to prevent abuse
- [ ] Use API keys or OAuth 2.0 for authentication
- [ ] Firewall: restrict Kafka (9092) and MongoDB (27017) to internal network only

### Input Validation

- [ ] Validate all event fields (userId, sessionId, eventType)
- [ ] Reject events older than 24 hours (clock skew)
- [ ] Sanitize error messages (never expose internal details)
- [ ] Use IP anonymization for user privacy

---

## Performance Targets

| Metric | Target | Strategy |
|--------|--------|----------|
| Event ingestion latency (p99) | < 10ms | Async Kafka, small batch window |
| Query latency (p99) | < 200ms | MongoDB indexes, pagination |
| Throughput | 10k+ events/sec | Kafka partitions, connection pooling |
| Availability | 99.9% | Replica sets, horizontal scaling |
| Error rate | < 0.1% | Rate limiting, validation, monitoring |

---

## Disaster Recovery

### Backup & Restore

**Automated daily backup:**
```bash
# MongoDB backup
mongodump --uri "mongodb://prod:pass@mongo:27017/clickstream_db" \
          --out /backups/$(date +%Y%m%d) | \
tar -czf - | aws s3 cp - s3://backups/mongodb-$(date +%Y%m%d).tar.gz

# Restore
aws s3 cp s3://backups/mongodb-2026-04-18.tar.gz - | tar -xzf -
mongorestore --uri "mongodb://..." /backups/2026-04-18
```

### Failover Plan

1. **Monitor health checks:** Alert on pod restart
2. **Auto-restart:** Kubernetes restarts failed pods
3. **Data recovery:** From MongoDB backup (if needed)
4. **Notify stakeholders:** Via PagerDuty/Slack

---

## Deployment Checklist

- [ ] Secrets configured in environment
- [ ] MongoDB indexes created
- [ ] Kafka topic replicated (factor 3)
- [ ] Application deployed (3+ replicas)
- [ ] Load balancer configured
- [ ] Monitoring dashboards visible
- [ ] Alert rules configured
- [ ] Backup strategy tested
- [ ] Security audit completed
- [ ] Performance benchmarks OK
- [ ] Rollback procedure documented
