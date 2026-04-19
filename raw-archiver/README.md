# Raw Event Archiver

Simple Kafka consumer that writes raw clickstream events to Parquet files in a date-partitioned data lake for long-term archival and future reprocessing.

## Features

- **Parquet columnar storage** with Snappy compression for efficient storage
- **Date-partitioned layout** (year/month/day/hour) for query optimization
- **Manual offset commit** ensures no data loss on failures
- **Buffered writes** with configurable flush thresholds (event count + time interval)
- **Thread-safe** buffer management with graceful shutdown

## Architecture

### Directory Layout
```
data-lake/
└── raw-events/
    └── year=2026/
        └── month=04/
            └── day=18/
                └── hour=14/
                    ├── part-00001-1713451200.snappy.parquet
                    ├── part-00002-1713451260.snappy.parquet
                    └── ...
```

### Parquet Schema

| Field | Type | Description |
|-------|------|-------------|
| eventId | string | Unique event identifier |
| userId | string | User identifier |
| sessionId | string | Session identifier |
| eventType | string | Event type (CLICK, PAGE_VIEW, etc.) |
| targetElement | string (nullable) | HTML element target |
| pageUrl | string | Page URL where event occurred |
| referrerUrl | string (nullable) | Referrer URL |
| timestamp | long | Event timestamp (epoch milliseconds) |
| userAgent | string (nullable) | Browser user agent |
| schemaVersion | string | Event schema version |

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| KAFKA_BOOTSTRAP_SERVERS | localhost:9056 | Kafka broker addresses |
| DATA_LAKE_PATH | ./data-lake | Base path for data lake |
| FLUSH_EVENT_THRESHOLD | 10000 | Flush after N events |
| FLUSH_INTERVAL_SECONDS | 60 | Flush after N seconds |

### application.yml

```yaml
archiver:
  topic: clickstream-events
  data-lake-base-path: ./data-lake
  flush:
    event-threshold: 10000
    time-interval-seconds: 60
  parquet:
    compression: SNAPPY
    page-size: 1048576      # 1MB
    row-group-size: 134217728  # 128MB
```

## Running

### Local Development

```bash
# Build the module
mvn clean package -pl raw-archiver

# Run with default settings
java -jar target/raw-archiver-1.0.0-SNAPSHOT.jar

# Run with custom data lake path
java -jar target/raw-archiver-1.0.0-SNAPSHOT.jar \
  --archiver.data-lake-base-path=/mnt/data-lake
```

### Docker Compose

The archiver is integrated into the main docker-compose.yml:

```bash
docker-compose up raw-archiver
```

## Data Loss Prevention

The archiver implements manual offset management to prevent data loss:

1. Events are buffered in memory
2. Parquet write is attempted
3. **Only after successful write**, Kafka offset is committed
4. On failure, events remain in buffer and offset is not committed
5. On restart, Kafka replays from last committed offset

## Monitoring

### Health Check

```bash
curl http://localhost:9053/actuator/health
```

Returns:
```json
{
  "status": "UP",
  "details": {
    "totalEventsProcessed": 123456,
    "totalBatchesFlushed": 13,
    "currentBufferSize": 5432,
    "lastFlush": "2026-04-18T14:30:00Z"
  }
}
```

## Querying Archived Data

### With DuckDB (easiest)

```sql
-- Query all events from a specific day
SELECT * FROM read_parquet('data-lake/raw-events/year=2026/month=04/day=18/**/*.parquet');

-- Count events by type
SELECT eventType, COUNT(*) 
FROM read_parquet('data-lake/raw-events/**/*.parquet')
GROUP BY eventType;
```

### With Spark

```python
df = spark.read.parquet("data-lake/raw-events/")
df.filter("year=2026 AND month=04").show()
```

## Performance Tuning

### File Sizes

Optimal Parquet file size is 50-200MB. Adjust flush thresholds:

- **Low traffic:** Increase `time-interval-seconds` (e.g., 300) to avoid many small files
- **High traffic:** Decrease `event-threshold` (e.g., 5000) to avoid huge files

### Memory Usage

Buffer size = `event-threshold` × avg event size (~500 bytes) = ~5MB at default settings.

## Troubleshooting

### Parquet files not appearing

1. Check Kafka connectivity: `docker-compose logs raw-archiver`
2. Verify topic name matches configuration
3. Check filesystem permissions on data-lake directory

### Buffer not flushing

- Check `time-interval-seconds` - may be too long
- Verify event count reaches `event-threshold`
- Check logs for write errors

### Data loss on crash

- Verify `enable-auto-commit: false` in application.yml
- Check `ack-mode: manual` is set
- Events in buffer during crash are lost (max 10,000 by default)

## Future Enhancements

- [ ] S3/GCS upload for cloud storage
- [ ] Parquet file compaction job for small files
- [ ] Partition pruning statistics
- [ ] Encryption at rest
- [ ] Retention policy automation
