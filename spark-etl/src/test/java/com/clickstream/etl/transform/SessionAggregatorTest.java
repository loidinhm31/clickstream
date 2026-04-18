package com.clickstream.etl.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SessionAggregator.
 */
class SessionAggregatorTest {
    
    private static SparkSession spark;
    private SessionAggregator aggregator;
    
    @BeforeAll
    static void initSpark() {
        spark = SparkSession.builder()
                .appName("SessionAggregatorTest")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }
    
    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }
    
    @BeforeEach
    void setUp() {
        aggregator = new SessionAggregator();
    }
    
    @Test
    @DisplayName("Should aggregate events into sessions with correct metrics")
    void testSessionAggregation() {
        // Arrange: Create test events for a single session
        long now = Instant.now().toEpochMilli();
        List<TestEvent> events = Arrays.asList(
                new TestEvent("evt1", "user1", "sess1", "PAGE_VIEW", 
                        "/home", now),
                new TestEvent("evt2", "user1", "sess1", "CLICK", 
                        "/home", now + 1000),
                new TestEvent("evt3", "user1", "sess1", "SCROLL", 
                        "/home", now + 2000),
                new TestEvent("evt4", "user1", "sess1", "PAGE_VIEW", 
                        "/about", now + 5000)
        );
        
        Dataset<Row> eventDf = createEventDataset(events);
        
        // Act: Apply session aggregation
        Dataset<Row> result = aggregator.aggregate(eventDf, 30);
        
        // Assert: Verify aggregated metrics
        long count = result.count();
        assertEquals(1, count, "Should produce one session aggregate");
        
        Row session = result.first();
        assertEquals("sess1", session.getAs("sessionId"));
        assertEquals("user1", session.getAs("userId"));
        assertEquals(2L, session.getAs("pageViewCount"));
        assertEquals(1L, session.getAs("clickCount"));
        assertEquals(1L, session.getAs("scrollEvents"));
        assertFalse(session.getAs("bounced"), "Session should not be bounced (2 pages)");
    }
    
    @Test
    @DisplayName("Should detect bounced sessions (single page < 10 seconds)")
    void testBouncedSession() {
        // Arrange: Create bounced session (single page, < 10 sec)
        long now = Instant.now().toEpochMilli();
        List<TestEvent> events = Arrays.asList(
                new TestEvent("evt1", "user1", "sess1", "PAGE_VIEW", 
                        "/landing", now),
                new TestEvent("evt2", "user1", "sess1", "CLICK", 
                        "/landing", now + 5000)  // Same page, 5 seconds later
        );
        
        Dataset<Row> eventDf = createEventDataset(events);
        
        // Act
        Dataset<Row> result = aggregator.aggregate(eventDf, 30);
        
        // Assert
        Row session = result.first();
        assertTrue(session.getAs("bounced"), 
                "Single-page session < 10s should be bounced");
    }
    
    @Test
    @DisplayName("Should separate sessions with gap > 30 minutes")
    void testSessionWindowGap() {
        // Arrange: Create events with 31-minute gap (should be 2 sessions)
        long now = Instant.now().toEpochMilli();
        long gapMs = 31 * 60 * 1000;  // 31 minutes
        
        List<TestEvent> events = Arrays.asList(
                new TestEvent("evt1", "user1", "sess1", "PAGE_VIEW", 
                        "/home", now),
                new TestEvent("evt2", "user1", "sess1", "PAGE_VIEW", 
                        "/home", now + gapMs)  // Different session after gap
        );
        
        Dataset<Row> eventDf = createEventDataset(events);
        
        // Act
        Dataset<Row> result = aggregator.aggregate(eventDf, 30);
        
        // Assert
        long count = result.count();
        assertEquals(2, count, "Events with >30 min gap should create separate sessions");
    }
    
    @Test
    @DisplayName("Should calculate entry and exit pages correctly")
    void testEntryExitPages() {
        // Arrange: Multi-page session
        long now = Instant.now().toEpochMilli();
        List<TestEvent> events = Arrays.asList(
                new TestEvent("evt1", "user1", "sess1", "PAGE_VIEW", 
                        "/entry", now),
                new TestEvent("evt2", "user1", "sess1", "PAGE_VIEW", 
                        "/middle", now + 5000),
                new TestEvent("evt3", "user1", "sess1", "PAGE_VIEW", 
                        "/exit", now + 10000)
        );
        
        Dataset<Row> eventDf = createEventDataset(events);
        
        // Act
        Dataset<Row> result = aggregator.aggregate(eventDf, 30);
        
        // Assert
        Row session = result.first();
        assertEquals("/entry", session.getAs("entryPage"));
        assertEquals("/exit", session.getAs("exitPage"));
    }
    
    // Helper methods
    
    private Dataset<Row> createEventDataset(List<TestEvent> events) {
        return spark.createDataFrame(events, TestEvent.class)
                .withColumnRenamed("eventId", "eventId")
                .withColumn("timestamp", 
                        org.apache.spark.sql.functions.expr("cast(timestamp / 1000 as timestamp)"))
                .withWatermark("timestamp", "10 minutes");
    }
    
    /**
     * Test event POJO for Spark DataFrame creation.
     */
    public static class TestEvent {
        private String eventId;
        private String userId;
        private String sessionId;
        private String eventType;
        private String pageUrl;
        private Long timestamp;
        
        public TestEvent() {}
        
        public TestEvent(String eventId, String userId, String sessionId,
                        String eventType, String pageUrl, Long timestamp) {
            this.eventId = eventId;
            this.userId = userId;
            this.sessionId = sessionId;
            this.eventType = eventType;
            this.pageUrl = pageUrl;
            this.timestamp = timestamp;
        }
        
        // Getters and setters required by Spark
        public String getEventId() { return eventId; }
        public void setEventId(String eventId) { this.eventId = eventId; }
        
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        
        public String getSessionId() { return sessionId; }
        public void setSessionId(String sessionId) { this.sessionId = sessionId; }
        
        public String getEventType() { return eventType; }
        public void setEventType(String eventType) { this.eventType = eventType; }
        
        public String getPageUrl() { return pageUrl; }
        public void setPageUrl(String pageUrl) { this.pageUrl = pageUrl; }
        
        public Long getTimestamp() { return timestamp; }
        public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
    }
}
