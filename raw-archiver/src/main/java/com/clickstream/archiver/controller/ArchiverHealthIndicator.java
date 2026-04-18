package com.clickstream.archiver.controller;

import com.clickstream.archiver.config.ArchiverConfig;
import com.clickstream.archiver.consumer.RawEventArchiver;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Health indicator for the archiver service.
 * Reports service status, statistics, and circuit breaker health.
 */
@Component
public class ArchiverHealthIndicator implements HealthIndicator {
    
    private final RawEventArchiver archiver;
    private final ArchiverConfig config;
    
    // Minimum stuck threshold (will be increased based on flush interval)
    private static final long MIN_STUCK_THRESHOLD_MINUTES = 2;
    
    public ArchiverHealthIndicator(RawEventArchiver archiver, ArchiverConfig config) {
        this.archiver = archiver;
        this.config = config;
    }
    
    @Override
    public Health health() {
        RawEventArchiver.ArchiverStats stats = archiver.getStats();
        
        // Dynamic threshold: 3x flush interval or 2 minutes (whichever is longer)
        long stuckThresholdMinutes = getStuckThresholdMinutes();
        
        // Check circuit breaker status
        if (stats.consecutiveFailures() >= 3) {
            return Health.down()
                    .withDetail("reason", "Circuit breaker open - 3 consecutive flush failures")
                    .withDetail("consecutiveFailures", stats.consecutiveFailures())
                    .withDetail("bufferSize", stats.currentBufferSize())
                    .withDetail("lastSuccessfulWrite", stats.lastSuccessfulWrite())
                    .withDetail("stuckThresholdMinutes", stuckThresholdMinutes)
                    .build();
        }
        
        // Check if service is stuck (no successful write in N minutes with buffered events)
        java.time.Duration sinceLastWrite = java.time.Duration.between(
                stats.lastSuccessfulWrite(), 
                java.time.Instant.now()
        );
        
        if (stats.currentBufferSize() > 0 && sinceLastWrite.toMinutes() >= stuckThresholdMinutes) {
            return Health.down()
                    .withDetail("reason", String.format("No successful write in %d+ minutes with buffered events", 
                            stuckThresholdMinutes))
                    .withDetail("bufferSize", stats.currentBufferSize())
                    .withDetail("minutesSinceLastWrite", sinceLastWrite.toMinutes())
                    .withDetail("consecutiveFailures", stats.consecutiveFailures())
                    .withDetail("totalEventsProcessed", stats.totalEventsProcessed())
                    .withDetail("totalBatchesFlushed", stats.totalBatchesFlushed())
                    .withDetail("stuckThresholdMinutes", stuckThresholdMinutes)
                    .build();
        }
        
        return Health.up()
                .withDetail("totalEventsProcessed", stats.totalEventsProcessed())
                .withDetail("totalBatchesFlushed", stats.totalBatchesFlushed())
                .withDetail("currentBufferSize", stats.currentBufferSize())
                .withDetail("lastSuccessfulWrite", stats.lastSuccessfulWrite())
                .withDetail("minutesSinceLastWrite", sinceLastWrite.toMinutes())
                .withDetail("consecutiveFailures", stats.consecutiveFailures())
                .withDetail("stuckThresholdMinutes", stuckThresholdMinutes)
                .build();
    }
    
    /**
     * Calculates dynamic stuck threshold based on flush configuration.
     * Returns max(MIN_STUCK_THRESHOLD_MINUTES, 3x flush interval) to avoid false positives
     * during low-traffic periods when buffer doesn't fill to threshold.
     */
    private long getStuckThresholdMinutes() {
        int flushIntervalSeconds = config.getFlush().getTimeIntervalSeconds();
        long dynamicThreshold = (flushIntervalSeconds * 3L) / 60;
        return Math.max(MIN_STUCK_THRESHOLD_MINUTES, dynamicThreshold);
    }
}
