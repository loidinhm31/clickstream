package com.clickstream.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.index.Indexed;

import java.time.Instant;
import java.util.List;

/**
 * MongoDB document representing a user's journey map.
 * 
 * Captures the sequence of pages visited during a session,
 * useful for funnel analysis and path optimization.
 * 
 * Indexes:
 * - Index on userId for user-specific journey queries
 * - Index on sessionId for session-specific journey lookup
 */
@Document(collection = "user_journeys")
public class UserJourney {
    
    @Id
    private String id;  // userId:sessionId
    
    @Indexed
    private String userId;
    
    @Indexed
    private String sessionId;
    
    private Instant startTime;
    
    private Instant endTime;
    
    // Ordered list of page visits
    private List<PageVisit> pageSequence;
    
    // Total journey duration
    private Long totalDurationMs;
    
    // Number of pages in journey
    private Integer pageCount;

    // Constructors
    public UserJourney() {}

    public UserJourney(String id, String userId, String sessionId, Instant startTime,
                       Instant endTime, List<PageVisit> pageSequence, Long totalDurationMs,
                       Integer pageCount) {
        this.id = id;
        this.userId = userId;
        this.sessionId = sessionId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.pageSequence = pageSequence;
        this.totalDurationMs = totalDurationMs;
        this.pageCount = pageCount;
    }

    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public Instant getStartTime() { return startTime; }
    public void setStartTime(Instant startTime) { this.startTime = startTime; }

    public Instant getEndTime() { return endTime; }
    public void setEndTime(Instant endTime) { this.endTime = endTime; }

    public List<PageVisit> getPageSequence() { return pageSequence; }
    public void setPageSequence(List<PageVisit> pageSequence) { this.pageSequence = pageSequence; }

    public Long getTotalDurationMs() { return totalDurationMs; }
    public void setTotalDurationMs(Long totalDurationMs) { this.totalDurationMs = totalDurationMs; }

    public Integer getPageCount() { return pageCount; }
    public void setPageCount(Integer pageCount) { this.pageCount = pageCount; }

    /**
     * Represents a single page visit within a journey
     */
    public static class PageVisit {
        private String pageUrl;
        private Instant timestamp;
        private Long durationMs;
        private Integer eventCount;

        public PageVisit() {}

        public PageVisit(String pageUrl, Instant timestamp, Long durationMs, Integer eventCount) {
            this.pageUrl = pageUrl;
            this.timestamp = timestamp;
            this.durationMs = durationMs;
            this.eventCount = eventCount;
        }

        public String getPageUrl() { return pageUrl; }
        public void setPageUrl(String pageUrl) { this.pageUrl = pageUrl; }

        public Instant getTimestamp() { return timestamp; }
        public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

        public Long getDurationMs() { return durationMs; }
        public void setDurationMs(Long durationMs) { this.durationMs = durationMs; }

        public Integer getEventCount() { return eventCount; }
        public void setEventCount(Integer eventCount) { this.eventCount = eventCount; }
    }
}
