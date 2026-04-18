package com.clickstream.realtime.engine;

import java.util.List;
import java.util.Map;

/**
 * Real-time metrics snapshot computed from sliding windows.
 * 
 * <p>This class represents the aggregated metrics that will be serialized
 * to Apache Arrow IPC format and sent to clients via WebSocket/HTTP.
 * 
 * <p>Metrics:
 * <ul>
 *   <li>Active Users: Distinct user count in last 5 minutes</li>
 *   <li>Clicks Per Second: Average click rate in last 1 minute</li>
 *   <li>Event Rate: Total events per second in last 1 minute</li>
 *   <li>Trending Pages: Top 10 pages by view count in last 15 minutes</li>
 * </ul>
 * 
 * @param activeUsers      Count of distinct users in 5-minute window
 * @param clicksPerSecond  Average clicks/second in 1-minute window
 * @param eventRate        Average events/second in 1-minute window
 * @param trendingPages    Top-N pages with counts (pageUrl -> viewCount)
 * @param computedAt       Timestamp when metrics were computed (epoch millis)
 */
public record ArrowMetricsSnapshot(
    int activeUsers,
    double clicksPerSecond,
    double eventRate,
    List<PageMetric> trendingPages,
    long computedAt
) {
    
    /**
     * Page metric for trending pages list.
     * 
     * @param pageUrl   Full URL of the page
     * @param viewCount Number of views in the window
     */
    public record PageMetric(String pageUrl, int viewCount) {}
    
    /**
     * Create an empty metrics snapshot (no data available).
     */
    public static ArrowMetricsSnapshot empty() {
        return new ArrowMetricsSnapshot(0, 0.0, 0.0, List.of(), System.currentTimeMillis());
    }
}
