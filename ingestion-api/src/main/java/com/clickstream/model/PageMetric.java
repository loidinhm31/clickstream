package com.clickstream.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.index.CompoundIndex;

/**
 * MongoDB document representing page-level metrics.
 * 
 * Matches the schema produced by Spark ETL (Phase 4).
 */
@Document(collection = "page_metrics")
@CompoundIndex(name = "page_window_idx", def = "{'pageUrl': 1, 'windowStart': -1}")
public class PageMetric {
    
    @Id
    private String id;  // composite key: pageUrl_windowStart
    
    private String pageUrl;
    
    private String windowStart;

    private String windowEnd;
    
    private Long totalViews;
    
    private Long uniqueVisitors;
    
    private Long clickCount;
    
    private Double avgScrollDepth;
    
    private Double bounceRate;

    // Constructors
    public PageMetric() {}

    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getPageUrl() { return pageUrl; }
    public void setPageUrl(String pageUrl) { this.pageUrl = pageUrl; }

    public String getWindowStart() { return windowStart; }
    public void setWindowStart(String windowStart) { this.windowStart = windowStart; }

    public String getWindowEnd() { return windowEnd; }
    public void setWindowEnd(String windowEnd) { this.windowEnd = windowEnd; }

    public Long getTotalViews() { return totalViews; }
    public void setTotalViews(Long totalViews) { this.totalViews = totalViews; }

    public Long getUniqueVisitors() { return uniqueVisitors; }
    public void setUniqueVisitors(Long uniqueVisitors) { this.uniqueVisitors = uniqueVisitors; }

    public Long getClickCount() { return clickCount; }
    public void setClickCount(Long clickCount) { this.clickCount = clickCount; }

    public Double getAvgScrollDepth() { return avgScrollDepth; }
    public void setAvgScrollDepth(Double avgScrollDepth) { this.avgScrollDepth = avgScrollDepth; }

    public Double getBounceRate() { return bounceRate; }
    public void setBounceRate(Double bounceRate) { this.bounceRate = bounceRate; }
}
