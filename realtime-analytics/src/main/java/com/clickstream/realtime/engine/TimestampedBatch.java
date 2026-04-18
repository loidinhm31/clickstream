package com.clickstream.realtime.engine;

import org.apache.arrow.vector.VectorSchemaRoot;
import java.time.Instant;

/**
 * Wrapper for an Arrow batch with its ingestion timestamp.
 * 
 * <p>Used in the MetricsEngine ring buffer to track batch age for eviction.
 * Each batch represents events from a specific time window (typically 1 second).
 * 
 * @param timestamp Instant when this batch was ingested into the ring buffer
 * @param batch     Arrow VectorSchemaRoot containing event columns (userId, sessionId, eventType, pageUrl, timestamp)
 */
public record TimestampedBatch(Instant timestamp, VectorSchemaRoot batch) {

    /**
     * Calculate age of this batch in milliseconds.
     * 
     * @param now Current time
     * @return Age in milliseconds
     */
    public long ageMillis(Instant now) {
        return now.toEpochMilli() - timestamp.toEpochMilli();
    }

    /**
     * Check if this batch is older than the specified threshold.
     * 
     * @param maxAgeMillis Maximum allowed age in milliseconds
     * @param now          Current time
     * @return true if batch should be evicted
     */
    public boolean isExpired(long maxAgeMillis, Instant now) {
        return ageMillis(now) > maxAgeMillis;
    }
}
