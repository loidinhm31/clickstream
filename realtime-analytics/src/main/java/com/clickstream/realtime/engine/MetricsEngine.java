package com.clickstream.realtime.engine;

import com.clickstream.model.ClickEvent;
import com.clickstream.model.EventType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

/**
 * Core metrics engine with Apache Arrow columnar storage.
 * 
 * <p>Architecture:
 * - Ring buffer of Arrow VectorSchemaRoot batches (max 900 batches = 15 minutes)
 * - Each batch contains events from a 1-second window
 * - Sliding window computations iterate over batches
 * - Automatic eviction of old batches beyond window threshold
 * 
 * <p>Thread Safety:
 * - ConcurrentLinkedDeque for ring buffer (thread-safe add/remove)
 * - Arrow buffer allocator is thread-safe
 * - VectorSchemaRoot access is synchronized during metric computation
 * 
 * <p>Memory Management:
 * - Arrow uses off-heap memory via Netty allocator
 * - Close evicted batches explicitly to release memory
 * - BufferAllocator tracks total memory usage with configurable limit
 */
@Component
public class MetricsEngine {

    private static final Logger logger = LoggerFactory.getLogger(MetricsEngine.class);

    // Arrow schema for event batches
    private static final Schema ARROW_SCHEMA = new Schema(Arrays.asList(
        new Field("userId", FieldType.nullable(new ArrowType.Utf8()), null),
        new Field("sessionId", FieldType.nullable(new ArrowType.Utf8()), null),
        new Field("eventType", FieldType.nullable(new ArrowType.Utf8()), null),
        new Field("pageUrl", FieldType.nullable(new ArrowType.Utf8()), null),
        new Field("timestamp", FieldType.nullable(new ArrowType.Int(64, true)), null)
    ));

    private final ConcurrentLinkedDeque<TimestampedBatch> ringBuffer = new ConcurrentLinkedDeque<>();
    private final BufferAllocator allocator;
    private final AtomicLong evictedBatchesCount = new AtomicLong(0);

    // Configuration
    private final int maxBatches;
    private final long activeUsersWindowMs;
    private final long clicksWindowMs;
    private final long trendingPagesWindowMs;
    private final long eventRateWindowMs;

    public MetricsEngine(
            @Value("${arrow.allocator.limit:536870912}") long allocatorLimit,
            @Value("${metrics.ring-buffer.max-batches:900}") int maxBatches,
            @Value("${metrics.windows.active-users-seconds:300}") int activeUsersSeconds,
            @Value("${metrics.windows.clicks-per-second:60}") int clicksPerSecondWindow,
            @Value("${metrics.windows.trending-pages-seconds:900}") int trendingPagesSeconds,
            @Value("${metrics.windows.event-rate-seconds:60}") int eventRateSeconds) {
        this.allocator = new RootAllocator(allocatorLimit);
        this.maxBatches = maxBatches;
        this.activeUsersWindowMs = activeUsersSeconds * 1000L;
        this.clicksWindowMs = clicksPerSecondWindow * 1000L;
        this.trendingPagesWindowMs = trendingPagesSeconds * 1000L;
        this.eventRateWindowMs = eventRateSeconds * 1000L;

        logger.info("MetricsEngine initialized: maxBatches={}, allocatorLimit={}MB",
                maxBatches, allocatorLimit / (1024 * 1024));
    }

    /**
     * Ingest a batch of events from Kafka.
     * Converts events to Arrow columnar format and adds to ring buffer.
     * 
     * @param events List of ClickEvents from Kafka consumer
     */
    public synchronized void ingestBatch(List<ClickEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }

        try {
            VectorSchemaRoot batch = buildArrowBatch(events);
            ringBuffer.addLast(new TimestampedBatch(Instant.now(), batch));
            evictOldBatches();

            logger.debug("Ingested batch: {} events, ring buffer size: {}",
                    events.size(), ringBuffer.size());
        } catch (Exception e) {
            logger.error("Failed to ingest batch", e);
        }
    }

    /**
     * Build Arrow VectorSchemaRoot from list of events.
     * Each event becomes a row in the columnar table.
     */
    private VectorSchemaRoot buildArrowBatch(List<ClickEvent> events) {
        VectorSchemaRoot root = VectorSchemaRoot.create(ARROW_SCHEMA, allocator);
        root.setRowCount(events.size());

        VarCharVector userIdVector = (VarCharVector) root.getVector("userId");
        VarCharVector sessionIdVector = (VarCharVector) root.getVector("sessionId");
        VarCharVector eventTypeVector = (VarCharVector) root.getVector("eventType");
        VarCharVector pageUrlVector = (VarCharVector) root.getVector("pageUrl");
        BigIntVector timestampVector = (BigIntVector) root.getVector("timestamp");

        for (int i = 0; i < events.size(); i++) {
            ClickEvent event = events.get(i);
            
            userIdVector.setSafe(i, event.getUserId().getBytes());
            sessionIdVector.setSafe(i, event.getSessionId().getBytes());
            eventTypeVector.setSafe(i, event.getEventType().name().getBytes());
            pageUrlVector.setSafe(i, event.getPageUrl().getBytes());
            timestampVector.setSafe(i, event.getTimestamp());
        }

        return root;
    }

    /**
     * Evict batches older than max window (15 minutes).
     * Releases Arrow memory by closing evicted VectorSchemaRoot.
     */
    private void evictOldBatches() {
        Instant now = Instant.now();
        long maxAgeMs = trendingPagesWindowMs; // Use longest window as threshold

        while (ringBuffer.size() > maxBatches) {
            TimestampedBatch oldest = ringBuffer.pollFirst();
            if (oldest != null) {
                oldest.batch().close();
                evictedBatchesCount.incrementAndGet();
                logger.debug("Evicted batch due to max size: age={}ms", oldest.ageMillis(now));
            }
        }

        // Evict expired batches
        Iterator<TimestampedBatch> iterator = ringBuffer.iterator();
        while (iterator.hasNext()) {
            TimestampedBatch batch = iterator.next();
            if (batch.isExpired(maxAgeMs, now)) {
                iterator.remove();
                batch.batch().close();
                evictedBatchesCount.incrementAndGet();
                logger.debug("Evicted expired batch: age={}ms", batch.ageMillis(now));
            } else {
                break; // Batches are ordered, stop when we hit a fresh one
            }
        }
    }

    /**
     * Compute real-time metrics from sliding windows.
     * 
     * <p>Scans all batches in ring buffer and aggregates metrics:
     * - Active users: distinct userIds in 5-minute window
     * - Clicks per second: CLICK events / 60 seconds
     * - Event rate: all events / 60 seconds
     * - Trending pages: top-10 pages by view count in 15-minute window
     * 
     * @return Snapshot of current metrics
     */
    public synchronized ArrowMetricsSnapshot computeMetrics() {
        long now = System.currentTimeMillis();
        
        Set<String> activeUsers = new HashSet<>();
        int clickCount = 0;
        int totalEvents = 0;
        Map<String, Integer> pageViews = new HashMap<>();

        for (TimestampedBatch tb : ringBuffer) {
            VectorSchemaRoot root = tb.batch();

            VarCharVector userIdVector = (VarCharVector) root.getVector("userId");
            VarCharVector eventTypeVector = (VarCharVector) root.getVector("eventType");
            VarCharVector pageUrlVector = (VarCharVector) root.getVector("pageUrl");
            BigIntVector timestampVector = (BigIntVector) root.getVector("timestamp");

            for (int i = 0; i < root.getRowCount(); i++) {
                long eventTs = timestampVector.get(i);
                long eventAge = now - eventTs;

                // Active users (5-minute window)
                if (eventAge < activeUsersWindowMs) {
                    String userId = new String(userIdVector.get(i), StandardCharsets.UTF_8);
                    activeUsers.add(userId);
                }

                // Clicks per second (1-minute window)
                if (eventAge < clicksWindowMs) {
                    totalEvents++;
                    String eventType = new String(eventTypeVector.get(i), StandardCharsets.UTF_8);
                    if ("CLICK".equals(eventType)) {
                        clickCount++;
                    }
                }

                // Trending pages (15-minute window)
                if (eventAge < trendingPagesWindowMs) {
                    String pageUrl = new String(pageUrlVector.get(i), StandardCharsets.UTF_8);
                    pageViews.merge(pageUrl, 1, Integer::sum);
                }
            }
        }

        // Calculate rates
        double clicksPerSecond = clickCount / (clicksWindowMs / 1000.0);
        double eventRate = totalEvents / (eventRateWindowMs / 1000.0);

        // Top-10 trending pages
        List<ArrowMetricsSnapshot.PageMetric> trendingPages = pageViews.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(10)
                .map(e -> new ArrowMetricsSnapshot.PageMetric(e.getKey(), e.getValue()))
                .collect(Collectors.toList());

        logger.debug("Computed metrics: activeUsers={}, clicksPerSec={}, eventRate={}, trendingPages={}",
                activeUsers.size(), clicksPerSecond, eventRate, trendingPages.size());

        return new ArrowMetricsSnapshot(
                activeUsers.size(),
                clicksPerSecond,
                eventRate,
                trendingPages,
                now
        );
    }

    /**
     * Get ring buffer statistics for monitoring.
     */
    public Map<String, Object> getStats() {
        return Map.of(
                "batchCount", (long) ringBuffer.size(),
                "allocatedMemoryMB", (long) (allocator.getAllocatedMemory() / (1024 * 1024)),
                "peakMemoryMB", (long) (allocator.getPeakMemoryAllocation() / (1024 * 1024)),
                "evictedBatches", evictedBatchesCount.get()
        );
    }

    /**
     * Clear all ingested data and release Arrow memory.
     * Useful for testing and manual state reset.
     */
    public synchronized void reset() {
        logger.info("Resetting MetricsEngine, clearing {} batches", ringBuffer.size());
        ringBuffer.forEach(tb -> {
            try {
                tb.batch().close();
            } catch (Exception e) {
                logger.error("Failed to close batch during reset", e);
            }
        });
        ringBuffer.clear();
        evictedBatchesCount.set(0);
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down MetricsEngine, releasing Arrow memory");
        ringBuffer.forEach(tb -> tb.batch().close());
        ringBuffer.clear();
        allocator.close();
    }
}
