package com.clickstream.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.index.CompoundIndex;

import java.time.Instant;
import java.util.Map;

/**
 * MongoDB document representing aggregated session metrics.
 * 
 * Written by: Spark ETL (Phase 4)
 * Read by: Ingestion API analytics endpoints
 * 
 * Indexes:
 * - Compound index on (userId, startTime) for user-specific queries
 * - Index on startTime for time-range queries
 */
@Document(collection = "session_aggregates")
@CompoundIndex(name = "user_time_idx", def = "{'userId': 1, 'startTime': -1}")
public class SessionAggregate {
    
    @Id
    private String id;  // sessionId
    
    @Indexed
    private String userId;
    
    @Indexed
    private Instant startTime;
    
    private Instant endTime;
    
    private Long durationMs;
    
    private Integer totalEvents;
    
    private Integer clickCount;
    
    private Integer pageViewCount;
    
    private Integer scrollCount;
    
    private Integer hoverCount;
    
    private String landingPage;
    
    private String exitPage;
    
    private Integer uniquePages;
    
    // Page URL -> visit count
    private Map<String, Integer> pageVisits;
    
    // Device/browser metadata
    private String userAgent;
    
    private String ipAddress;

    // Constructors
    public SessionAggregate() {}

    public SessionAggregate(String id, String userId, Instant startTime, Instant endTime, 
                            Long durationMs, Integer totalEvents, Integer clickCount, 
                            Integer pageViewCount, Integer scrollCount, Integer hoverCount,
                            String landingPage, String exitPage, Integer uniquePages,
                            Map<String, Integer> pageVisits, String userAgent, String ipAddress) {
        this.id = id;
        this.userId = userId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.durationMs = durationMs;
        this.totalEvents = totalEvents;
        this.clickCount = clickCount;
        this.pageViewCount = pageViewCount;
        this.scrollCount = scrollCount;
        this.hoverCount = hoverCount;
        this.landingPage = landingPage;
        this.exitPage = exitPage;
        this.uniquePages = uniquePages;
        this.pageVisits = pageVisits;
        this.userAgent = userAgent;
        this.ipAddress = ipAddress;
    }

    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public Instant getStartTime() { return startTime; }
    public void setStartTime(Instant startTime) { this.startTime = startTime; }

    public Instant getEndTime() { return endTime; }
    public void setEndTime(Instant endTime) { this.endTime = endTime; }

    public Long getDurationMs() { return durationMs; }
    public void setDurationMs(Long durationMs) { this.durationMs = durationMs; }

    public Integer getTotalEvents() { return totalEvents; }
    public void setTotalEvents(Integer totalEvents) { this.totalEvents = totalEvents; }

    public Integer getClickCount() { return clickCount; }
    public void setClickCount(Integer clickCount) { this.clickCount = clickCount; }

    public Integer getPageViewCount() { return pageViewCount; }
    public void setPageViewCount(Integer pageViewCount) { this.pageViewCount = pageViewCount; }

    public Integer getScrollCount() { return scrollCount; }
    public void setScrollCount(Integer scrollCount) { this.scrollCount = scrollCount; }

    public Integer getHoverCount() { return hoverCount; }
    public void setHoverCount(Integer hoverCount) { this.hoverCount = hoverCount; }

    public String getLandingPage() { return landingPage; }
    public void setLandingPage(String landingPage) { this.landingPage = landingPage; }

    public String getExitPage() { return exitPage; }
    public void setExitPage(String exitPage) { this.exitPage = exitPage; }

    public Integer getUniquePages() { return uniquePages; }
    public void setUniquePages(Integer uniquePages) { this.uniquePages = uniquePages; }

    public Map<String, Integer> getPageVisits() { return pageVisits; }
    public void setPageVisits(Map<String, Integer> pageVisits) { this.pageVisits = pageVisits; }

    public String getUserAgent() { return userAgent; }
    public void setUserAgent(String userAgent) { this.userAgent = userAgent; }

    public String getIpAddress() { return ipAddress; }
    public void setIpAddress(String ipAddress) { this.ipAddress = ipAddress; }
}
