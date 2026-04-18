package com.clickstream.realtime.engine;

import com.clickstream.model.ClickEvent;
import com.clickstream.model.EventType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for MetricsEngine.
 * 
 * <p>Tests cover:
 * - Ring buffer ingestion and eviction
 * - Sliding window metric computations
 * - Memory management (Arrow buffer cleanup)
 * - Edge cases (empty events, old events, high volume)
 */
@DisplayName("MetricsEngine Unit Tests")
class MetricsEngineTest {

    private MetricsEngine engine;

    @BeforeEach
    void setUp() {
        // Initialize with small windows for faster tests
        engine = new MetricsEngine(
                67108864L,  // 64MB allocator limit
                60,         // max 60 batches
                30,         // 30s active users window
                10,         // 10s clicks window
                60,         // 60s trending pages window
                10          // 10s event rate window
        );
    }

    @AfterEach
    void tearDown() {
        engine.shutdown();
    }

    @Test
    @DisplayName("Should ingest batch and update ring buffer")
    void testIngestBatch() {
        List<ClickEvent> events = createTestEvents(10);
        
        engine.ingestBatch(events);
        
        Map<String, Object> stats = engine.getStats();
        assertEquals(1L, (Long) stats.get("batchCount"));
        assertTrue((Long) stats.get("allocatedMemoryMB") >= 0);
    }

    @Test
    @DisplayName("Should compute metrics with empty ring buffer")
    void testComputeMetricsEmpty() {
        ArrowMetricsSnapshot metrics = engine.computeMetrics();
        
        assertEquals(0, metrics.activeUsers());
        assertEquals(0.0, metrics.clicksPerSecond());
        assertEquals(0.0, metrics.eventRate());
        assertTrue(metrics.trendingPages().isEmpty());
    }

    @Test
    @DisplayName("Should compute active users correctly")
    void testActiveUsersMetric() {
        // Create events from 3 distinct users
        List<ClickEvent> batch1 = List.of(
                createEvent("user1", "sess1", EventType.PAGE_VIEW, "/home"),
                createEvent("user2", "sess2", EventType.CLICK, "/home"),
                createEvent("user3", "sess3", EventType.PAGE_VIEW, "/about")
        );
        
        engine.ingestBatch(batch1);
        
        ArrowMetricsSnapshot metrics = engine.computeMetrics();
        assertEquals(3, metrics.activeUsers());
    }

    @Test
    @DisplayName("Should compute clicks per second correctly")
    void testClicksPerSecondMetric() {
        // Create 5 CLICK events
        List<ClickEvent> events = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            events.add(createEvent("user" + i, "sess" + i, EventType.CLICK, "/home"));
        }
        
        engine.ingestBatch(events);
        
        ArrowMetricsSnapshot metrics = engine.computeMetrics();
        // 5 clicks in 10-second window = 0.5 clicks/second
        assertTrue(metrics.clicksPerSecond() > 0);
    }

    @Test
    @DisplayName("Should compute event rate correctly")
    void testEventRateMetric() {
        List<ClickEvent> events = createTestEvents(20);
        
        engine.ingestBatch(events);
        
        ArrowMetricsSnapshot metrics = engine.computeMetrics();
        // 20 events in 10-second window = 2.0 events/second
        assertTrue(metrics.eventRate() > 0);
    }

    @Test
    @DisplayName("Should compute trending pages correctly")
    void testTrendingPagesMetric() {
        List<ClickEvent> events = List.of(
                createEvent("user1", "sess1", EventType.PAGE_VIEW, "/page-a"),
                createEvent("user1", "sess1", EventType.PAGE_VIEW, "/page-a"),
                createEvent("user1", "sess1", EventType.PAGE_VIEW, "/page-a"),
                createEvent("user2", "sess2", EventType.PAGE_VIEW, "/page-b"),
                createEvent("user2", "sess2", EventType.PAGE_VIEW, "/page-b"),
                createEvent("user3", "sess3", EventType.PAGE_VIEW, "/page-c")
        );
        
        engine.ingestBatch(events);
        
        ArrowMetricsSnapshot metrics = engine.computeMetrics();
        List<ArrowMetricsSnapshot.PageMetric> trending = metrics.trendingPages();
        
        assertFalse(trending.isEmpty());
        // page-a should be first with 3 views
        assertEquals("/page-a", trending.get(0).pageUrl());
        assertEquals(3, trending.get(0).viewCount());
    }

    @Test
    @DisplayName("Should evict old batches from ring buffer")
    void testBatchEviction() throws InterruptedException {
        // Fill ring buffer beyond max size
        for (int i = 0; i < 70; i++) {
            engine.ingestBatch(createTestEvents(5));
            Thread.sleep(10); // Small delay to ensure batch ordering
        }
        
        Map<String, Object> stats = engine.getStats();
        long batchCount = (Long) stats.get("batchCount");
        
        // Should not exceed max batches (60)
        assertTrue(batchCount <= 60L);
    }

    @Test
    @DisplayName("Should handle null or empty event lists")
    void testNullAndEmptyEvents() {
        engine.ingestBatch(null);
        engine.ingestBatch(List.of());
        
        ArrowMetricsSnapshot metrics = engine.computeMetrics();
        assertEquals(0, metrics.activeUsers());
    }

    @Test
    @DisplayName("Should deduplicate users in active users metric")
    void testActiveUsersDeduplication() {
        List<ClickEvent> events = List.of(
                createEvent("user1", "sess1", EventType.PAGE_VIEW, "/home"),
                createEvent("user1", "sess1", EventType.CLICK, "/home"),
                createEvent("user1", "sess1", EventType.SCROLL, "/home"),
                createEvent("user2", "sess2", EventType.PAGE_VIEW, "/about")
        );
        
        engine.ingestBatch(events);
        
        ArrowMetricsSnapshot metrics = engine.computeMetrics();
        // Should count 2 distinct users, not 4
        assertEquals(2, metrics.activeUsers());
    }

    @Test
    @DisplayName("Should limit trending pages to top 10")
    void testTrendingPagesLimit() {
        List<ClickEvent> events = new ArrayList<>();
        
        // Create 15 different pages with varying view counts
        for (int i = 0; i < 15; i++) {
            for (int j = 0; j < (15 - i); j++) {
                events.add(createEvent("user1", "sess1", EventType.PAGE_VIEW, "/page-" + i));
            }
        }
        
        engine.ingestBatch(events);
        
        ArrowMetricsSnapshot metrics = engine.computeMetrics();
        List<ArrowMetricsSnapshot.PageMetric> trending = metrics.trendingPages();
        
        // Should only return top 10 pages
        assertTrue(trending.size() <= 10);
        
        // Verify descending order
        for (int i = 0; i < trending.size() - 1; i++) {
            assertTrue(trending.get(i).viewCount() >= trending.get(i + 1).viewCount());
        }
    }

    // Helper methods

    private List<ClickEvent> createTestEvents(int count) {
        List<ClickEvent> events = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            events.add(createEvent("user" + i, "sess" + i, EventType.PAGE_VIEW, "/test"));
        }
        return events;
    }

    private ClickEvent createEvent(String userId, String sessionId, EventType type, String pageUrl) {
        return ClickEvent.builder()
                .eventId("evt-" + System.nanoTime())
                .userId(userId)
                .sessionId(sessionId)
                .eventType(type)
                .targetElement(type == EventType.CLICK ? "button#test" : null)
                .pageUrl(pageUrl)
                .referrerUrl(null)
                .timestamp(System.currentTimeMillis())
                .userAgent("Test-Agent")
                .metadata(null)
                .build();
    }
}
