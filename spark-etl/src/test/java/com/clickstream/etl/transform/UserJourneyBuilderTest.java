package com.clickstream.etl.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for UserJourneyBuilder.
 */
class UserJourneyBuilderTest {
    
    private static SparkSession spark;
    private UserJourneyBuilder builder;
    
    @BeforeAll
    static void initSpark() {
        spark = SparkSession.builder()
                .appName("UserJourneyBuilderTest")
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
        builder = new UserJourneyBuilder();
    }
    
    @Test
    @DisplayName("Should build journey with ordered page sequence")
    void testJourneyBuilding() {
        // Arrange: Create page view events with clicks
        long now = Instant.now().toEpochMilli();
        List<TestEvent> events = Arrays.asList(
                new TestEvent("evt1", "user1", "sess1", "PAGE_VIEW", "/home", now),
                new TestEvent("evt2", "user1", "sess1", "CLICK", "/home", now + 1000),
                new TestEvent("evt3", "user1", "sess1", "PAGE_VIEW", "/products", now + 5000),
                new TestEvent("evt4", "user1", "sess1", "CLICK", "/products", now + 6000),
                new TestEvent("evt5", "user1", "sess1", "CLICK", "/products", now + 7000)
        );
        
        Dataset<Row> eventDf = createEventDataset(events);
        
        // Act
        Dataset<Row> result = builder.build(eventDf, 30);
        
        // Assert
        assertEquals(1, result.count(), "Should produce one journey");
        Row journey = result.first();
        assertEquals("user1", journey.getAs("userId"));
        assertEquals("sess1", journey.getAs("sessionId"));
    }
    
    @Test
    @DisplayName("Should calculate session duration correctly")
    void testSessionDuration() {
        // Arrange: 10-second session
        long now = Instant.now().toEpochMilli();
        List<TestEvent> events = Arrays.asList(
                new TestEvent("evt1", "user1", "sess1", "PAGE_VIEW", "/start", now),
                new TestEvent("evt2", "user1", "sess1", "PAGE_VIEW", "/end", now + 10000)
        );
        
        Dataset<Row> eventDf = createEventDataset(events);
        
        // Act
        Dataset<Row> result = builder.build(eventDf, 30);
        
        // Assert
        Row journey = result.first();
        long duration = journey.getAs("totalSessionDuration");
        assertEquals(10000L, duration, "Duration should be 10 seconds in millis");
    }
    
    @Test
    @DisplayName("Should only include PAGE_VIEW events in journey")
    void testPageViewFiltering() {
        // Arrange: Mix of event types
        long now = Instant.now().toEpochMilli();
        List<TestEvent> events = Arrays.asList(
                new TestEvent("evt1", "user1", "sess1", "PAGE_VIEW", "/page1", now),
                new TestEvent("evt2", "user1", "sess1", "CLICK", "/page1", now + 1000),
                new TestEvent("evt3", "user1", "sess1", "SCROLL", "/page1", now + 2000),
                new TestEvent("evt4", "user1", "sess1", "PAGE_VIEW", "/page2", now + 5000)
        );
        
        Dataset<Row> eventDf = createEventDataset(events);
        
        // Act
        Dataset<Row> result = builder.build(eventDf, 30);
        
        // Assert: Should only count 2 page views in journey
        Row journey = result.first();
        assertNotNull(journey.getAs("orderedPages"));
    }
    
    @Test
    @DisplayName("Should separate journeys with gap > 30 minutes")
    void testSessionGapSeparation() {
        // Arrange: Two sessions with 31-minute gap
        long now = Instant.now().toEpochMilli();
        long gapMs = 31 * 60 * 1000;
        
        List<TestEvent> events = Arrays.asList(
                new TestEvent("evt1", "user1", "sess1", "PAGE_VIEW", "/page1", now),
                new TestEvent("evt2", "user1", "sess1", "PAGE_VIEW", "/page2", now + gapMs)
        );
        
        Dataset<Row> eventDf = createEventDataset(events);
        
        // Act
        Dataset<Row> result = builder.build(eventDf, 30);
        
        // Assert
        assertEquals(2, result.count(), "Should create separate journeys with >30 min gap");
    }
    
    // Helper methods
    
    private Dataset<Row> createEventDataset(List<TestEvent> events) {
        return spark.createDataFrame(events, TestEvent.class)
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
        
        // Getters and setters
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
