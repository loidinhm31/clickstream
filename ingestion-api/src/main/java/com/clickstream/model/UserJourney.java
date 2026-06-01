package com.clickstream.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.index.Indexed;

import java.util.List;

/**
 * MongoDB document representing a user's journey map.
 * 
 * Matches the schema produced by Spark ETL (Phase 4).
 */
@Document(collection = "user_journeys")
public class UserJourney {
    
    @Id
    private String id;  // composite key: userId_sessionId
    
    @Indexed
    private String userId;
    
    @Indexed
    private String sessionId;
    
    private String windowStart;

    private String windowEnd;
    
    // Ordered list of page visits
    private List<OrderedPage> orderedPages;
    
    // Total journey duration
    private Long totalSessionDuration;

    // Constructors
    public UserJourney() {}

    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public String getWindowStart() { return windowStart; }
    public void setWindowStart(String windowStart) { this.windowStart = windowStart; }

    public String getWindowEnd() { return windowEnd; }
    public void setWindowEnd(String windowEnd) { this.windowEnd = windowEnd; }

    public List<OrderedPage> getOrderedPages() { return orderedPages; }
    public void setOrderedPages(List<OrderedPage> orderedPages) { this.orderedPages = orderedPages; }

    public Long getTotalSessionDuration() { return totalSessionDuration; }
    public void setTotalSessionDuration(Long totalSessionDuration) { this.totalSessionDuration = totalSessionDuration; }

    /**
     * Represents a single page visit within a journey
     */
    public static class OrderedPage {
        private String pageUrl;
        private Long timestamp;
        private Integer clicksOnPage;

        public OrderedPage() {}

        public String getPageUrl() { return pageUrl; }
        public void setPageUrl(String pageUrl) { this.pageUrl = pageUrl; }

        public Long getTimestamp() { return timestamp; }
        public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }

        public Integer getClicksOnPage() { return clicksOnPage; }
        public void setClicksOnPage(Integer clicksOnPage) { this.clicksOnPage = clicksOnPage; }
    }
}
