package com.clickstream.archiver.controller;

import com.clickstream.archiver.consumer.RawEventArchiver;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Health indicator for the archiver service.
 * Reports service status and statistics.
 */
@Component
@RequiredArgsConstructor
public class ArchiverHealthIndicator implements HealthIndicator {
    
    private final RawEventArchiver archiver;
    
    @Override
    public Health health() {
        RawEventArchiver.ArchiverStats stats = archiver.getStats();
        
        // Check if service is stuck (no flush in 5 minutes with buffered events)
        java.time.Duration sinceLastFlush = java.time.Duration.between(
                stats.lastFlush(), 
                java.time.Instant.now()
        );
        
        if (stats.currentBufferSize() > 0 && sinceLastFlush.toMinutes() > 5) {
            return Health.down()
                    .withDetail("reason", "No flush in 5+ minutes with buffered events - service may be stuck")
                    .withDetail("bufferSize", stats.currentBufferSize())
                    .withDetail("minutesSinceLastFlush", sinceLastFlush.toMinutes())
                    .withDetail("totalEventsProcessed", stats.totalEventsProcessed())
                    .withDetail("totalBatchesFlushed", stats.totalBatchesFlushed())
                    .build();
        }
        
        return Health.up()
                .withDetail("totalEventsProcessed", stats.totalEventsProcessed())
                .withDetail("totalBatchesFlushed", stats.totalBatchesFlushed())
                .withDetail("currentBufferSize", stats.currentBufferSize())
                .withDetail("lastFlush", stats.lastFlush())
                .withDetail("minutesSinceLastFlush", sinceLastFlush.toMinutes())
                .build();
    }
}
