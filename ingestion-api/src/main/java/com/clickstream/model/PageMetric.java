package com.clickstream.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.index.CompoundIndex;

import java.time.Instant;

/**
 * MongoDB document representing page-level metrics.
 * 
 * Aggregated by: Spark ETL using tumbling windows (e.g., 1 hour)
 * Read by: Ingestion API for page analytics queries
 * 
 * Indexes:
 * - Compound index on (pageUrl, windowStart) for efficient time-series queries
 */
@Document(collection = "page_metrics")
@CompoundIndex(name = "page_window_idx", def = "{'pageUrl': 1, 'windowStart': -1}")
public class PageMetric {
    
    @Id
    private String id;  // pageUrl:windowStart
    
    private String pageUrl;
    
    private Instant windowStart;
    
    private Instant windowEnd;
    
    private Integer totalViews;
    
    private Integer uniqueVisitors;
    
    private Integer totalClicks;
    
    private Integer totalScrolls;
    
    private Integer totalHovers;
    
    private Long avgDurationMs;
    
    private Integer bounceCount;  // Sessions with only this page
    
    private Double bounceRate;  // bounceCount / totalViews

    // Constructors
    public PageMetric() {}

    public PageMetric(String id, String pageUrl, Instant windowStart, Instant windowEnd,
                      Integer totalViews, Integer uniqueVisitors, Integer totalClicks,
                      Integer totalScrolls, Integer totalHovers, Long avgDurationMs,
                      Integer bounceCount, Double bounceRate) {
        this.id = id;
        this.pageUrl = pageUrl;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.totalViews = totalViews;
        this.uniqueVisitors = uniqueVisitors;
        this.totalClicks = totalClicks;
        this.totalScrolls = totalScrolls;
        this.totalHovers = totalHovers;
        this.avgDurationMs = avgDurationMs;
        this.bounceCount = bounceCount;
        this.bounceRate = bounceRate;
    }

    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getPageUrl() { return pageUrl; }
    public void setPageUrl(String pageUrl) { this.pageUrl = pageUrl; }

    public Instant getWindowStart() { return windowStart; }
    public void setWindowStart(Instant windowStart) { this.windowStart = windowStart; }

    public Instant getWindowEnd() { return windowEnd; }
    public void setWindowEnd(Instant windowEnd) { this.windowEnd = windowEnd; }

    public Integer getTotalViews() { return totalViews; }
    public void setTotalViews(Integer totalViews) { this.totalViews = totalViews; }

    public Integer getUniqueVisitors() { return uniqueVisitors; }
    public void setUniqueVisitors(Integer uniqueVisitors) { this.uniqueVisitors = uniqueVisitors; }

    public Integer getTotalClicks() { return totalClicks; }
    public void setTotalClicks(Integer totalClicks) { this.totalClicks = totalClicks; }

    public Integer getTotalScrolls() { return totalScrolls; }
    public void setTotalScrolls(Integer totalScrolls) { this.totalScrolls = totalScrolls; }

    public Integer getTotalHovers() { return totalHovers; }
    public void setTotalHovers(Integer totalHovers) { this.totalHovers = totalHovers; }

    public Long getAvgDurationMs() { return avgDurationMs; }
    public void setAvgDurationMs(Long avgDurationMs) { this.avgDurationMs = avgDurationMs; }

    public Integer getBounceCount() { return bounceCount; }
    public void setBounceCount(Integer bounceCount) { this.bounceCount = bounceCount; }

    public Double getBounceRate() { return bounceRate; }
    public void setBounceRate(Double bounceRate) { this.bounceRate = bounceRate; }
}
