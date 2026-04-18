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
 * Unit tests for PageMetricsAggregator.
 */
class PageMetricsAggregatorTest {
    
    private static SparkSession spark;
    private PageMetricsAggregator aggregator;
    
    @BeforeAll
    static void initSpark() {
        spark = SparkSession.builder()
                .appName("PageMetricsAggregatorTest")
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
        aggregator = new PageMetricsAggregator();
    }
    
    @Test
    @DisplayName("Should aggregate page views and clicks per 5-minute window")
    void testPageMetricsAggregation() {
        // Arrange: Create events for single page in single window
        long now = Instant.now().toEpochMilli();
        String pageUrl = "/products";
        
        List<TestEvent> events = Arrays.asList(
                new TestEvent("evt1", "user1", "sess1", "PAGE_VIEW", pageUrl, now),
                new TestEvent("evt2", "user2", "sess2", "PAGE_VIEW", pageUrl, now + 1000),
                new TestEvent("evt3", "user1", "sess1", "CLICK", pageUrl, now + 2000),
                new TestEvent("evt4", "user1", "sess1", "CLICK", pageUrl, now + 3000)
        );
        
        Dataset<Row> eventDf = createEventDataset(events);
        
        // Act
        Dataset<Row> result = aggregator.aggregate(eventDf, 5);
        
        // Assert
        long count = result.count();
        assertEquals(1, count, "Should produce one page metric");
        
        Row metrics = result.first();
        assertEquals(pageUrl, metrics.getAs("pageUrl"));
        assertEquals(2L, metrics.getAs("totalViews"), "Should count 2 page views");
        assertEquals(2L, metrics.getAs("clickCount"), "Should count 2 clicks");
    }
    
    @Test
    @DisplayName("Should count unique visitors per page")
    void testUniqueVisitors() {
        // Arrange: Multiple users visiting same page
        long now = Instant.now().toEpochMilli();
        String pageUrl = "/about";
        
        List<TestEvent> events = Arrays.asList(
                new TestEvent("evt1", "user1", "sess1", "PAGE_VIEW", pageUrl, now),
                new TestEvent("evt2", "user2", "sess2", "PAGE_VIEW", pageUrl, now + 1000),
                new TestEvent("evt3", "user1", "sess1", "PAGE_VIEW", pageUrl, now + 2000),  // Duplicate user
                new TestEvent("evt4", "user3", "sess3", "PAGE_VIEW", pageUrl, now + 3000)
        );
        
        Dataset<Row> eventDf = createEventDataset(events);
        
        // Act
        Dataset<Row> result = aggregator.aggregate(eventDf, 5);
        
        // Assert
        Row metrics = result.first();
        // approx_count_distinct may return 3 (exact count of unique users)
        long uniqueVisitors = metrics.getAs("uniqueVisitors");
        assertTrue(uniqueVisitors >= 3 && uniqueVisitors <= 3, 
                "Should count 3 unique visitors");
    }
    
    @Test
    @DisplayName("Should separate metrics by 5-minute windows")
    void testWindowSeparation() {
        // Arrange: Events in two different 5-minute windows
        long now = Instant.now().toEpochMilli();
        long window2 = now + (6 * 60 * 1000);  // 6 minutes later (different window)
        String pageUrl = "/home";
        
        List<TestEvent> events = Arrays.asList(
                new TestEvent("evt1", "user1", "sess1", "PAGE_VIEW", pageUrl, now),
                new TestEvent("evt2", "user1", "sess1", "PAGE_VIEW", pageUrl, window2)
        );
        
        Dataset<Row> eventDf = createEventDataset(events);
        
        // Act
        Dataset<Row> result = aggregator.aggregate(eventDf, 5);
        
        // Assert
        long count = result.count();
        assertEquals(2, count, "Should create separate metrics for different windows");
    }
    
    @Test
    @DisplayName("Should aggregate metrics for different pages separately")
    void testMultiplePages() {
        // Arrange: Events for different pages
        long now = Instant.now().toEpochMilli();
        
        List<TestEvent> events = Arrays.asList(
                new TestEvent("evt1", "user1", "sess1", "PAGE_VIEW", "/page1", now),
                new TestEvent("evt2", "user1", "sess1", "PAGE_VIEW", "/page2", now + 1000),
                new TestEvent("evt3", "user2", "sess2", "PAGE_VIEW", "/page1", now + 2000)
        );
        
        Dataset<Row> eventDf = createEventDataset(events);
        
        // Act
        Dataset<Row> result = aggregator.aggregate(eventDf, 5);
        
        // Assert
        long count = result.count();
        assertEquals(2, count, "Should create separate metrics for each page");
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
