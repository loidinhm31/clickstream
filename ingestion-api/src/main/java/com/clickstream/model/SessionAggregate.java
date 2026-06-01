package com.clickstream.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;

/**
 * MongoDB document representing aggregated session metrics.
 * 
 * Matches the schema produced by Spark ETL (Phase 4).
 */
@Document(collection = "session_aggregates")
@CompoundIndex(name = "user_time_idx", def = "{'userId': 1, 'windowStart': -1}")
public class SessionAggregate {
    
    @Id
    private String id;  // composite key from Spark: sessionId_windowStart
    
    @Indexed
    @Field("sessionId")
    private String sessionId;
    
    @Indexed
    private String userId;
    
    @Indexed
    private String windowStart;

    private String windowEnd;
    
    private Long durationMs;
    
    private Integer pageViewCount;
    
    private Integer clickCount;
    
    private Integer scrollEvents;
    
    private List<String> uniquePages;
    
    private String entryPage;
    
    private String exitPage;
    
    private Boolean bounced;

    // Constructors
    public SessionAggregate() {}

    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getWindowStart() { return windowStart; }
    public void setWindowStart(String windowStart) { this.windowStart = windowStart; }

    public String getWindowEnd() { return windowEnd; }
    public void setWindowEnd(String windowEnd) { this.windowEnd = windowEnd; }

    public Long getDurationMs() { return durationMs; }
    public void setDurationMs(Long durationMs) { this.durationMs = durationMs; }

    public Integer getPageViewCount() { return pageViewCount; }
    public void setPageViewCount(Integer pageViewCount) { this.pageViewCount = pageViewCount; }

    public Integer getClickCount() { return clickCount; }
    public void setClickCount(Integer clickCount) { this.clickCount = clickCount; }

    public Integer getScrollEvents() { return scrollEvents; }
    public void setScrollEvents(Integer scrollEvents) { this.scrollEvents = scrollEvents; }

    public List<String> getUniquePages() { return uniquePages; }
    public void setUniquePages(List<String> uniquePages) { this.uniquePages = uniquePages; }

    public String getEntryPage() { return entryPage; }
    public void setEntryPage(String entryPage) { this.entryPage = entryPage; }

    public String getExitPage() { return exitPage; }
    public void setExitPage(String exitPage) { this.exitPage = exitPage; }

    public Boolean getBounced() { return bounced; }
    public void setBounced(Boolean bounced) { this.bounced = bounced; }
}
