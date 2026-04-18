package com.clickstream.etl.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

/**
 * Transforms raw clickstream events into page-level metrics.
 * 
 * <p>Produces records with structure:
 * <pre>
 * {
 *   pageUrl: string,
 *   windowStart: timestamp,
 *   windowEnd: timestamp,
 *   totalViews: long,
 *   uniqueVisitors: long,
 *   clickCount: long,
 *   avgScrollDepth: double,
 *   bounceRate: double
 * }
 * </pre>
 * 
 * <p>Window type: 5-minute tumbling window (fixed-size, non-overlapping).
 */
@Component
public class PageMetricsAggregator {

    /**
     * Transforms raw events into page-level metrics.
     * 
     * @param rawEvents DataFrame with watermarked timestamp column
     * @param windowMinutes tumbling window duration in minutes
     * @return aggregated DataFrame ready for MongoDB sink
     */
    public Dataset<Row> aggregate(Dataset<Row> rawEvents, int windowMinutes) {
        String windowDuration = windowMinutes + " minutes";
        
        // First pass: aggregate metrics per page per window
        Dataset<Row> pageMetrics = rawEvents
                .groupBy(
                        window(col("timestamp"), windowDuration),
                        col("pageUrl"))
                .agg(
                        // Window bounds
                        col("window.start").alias("windowStart"),
                        col("window.end").alias("windowEnd"),
                        
                        // Page view count
                        count(when(col("eventType").equalTo("PAGE_VIEW"), 1))
                                .alias("totalViews"),
                        
                        // Unique visitor count (approximate for performance)
                        approx_count_distinct("userId").alias("uniqueVisitors"),
                        
                        // Click count
                        count(when(col("eventType").equalTo("CLICK"), 1))
                                .alias("clickCount"),
                        
                        // Average scroll depth
                        avg(when(col("eventType").equalTo("SCROLL"), 
                                col("metadata.scrollDepth")))
                                .alias("avgScrollDepth")
                )
                .drop("window");
        
        // Second pass: calculate bounce rate (sessions with only 1 page AND < 10 sec)
        // Create a session-level bounce indicator first
        Dataset<Row> sessionBounces = rawEvents
                .groupBy(
                        window(col("timestamp"), windowDuration),
                        col("sessionId"),
                        col("pageUrl"))
                .agg(
                        col("window.start").alias("windowStart"),
                        collect_set("pageUrl").alias("pagesInSession"),
                        // Calculate duration in seconds (timestamps are already Spark timestamp type)
                        expr("max(unix_timestamp(timestamp)) - min(unix_timestamp(timestamp))")
                                .alias("sessionDurationSeconds")
                )
                .withColumn("isBounce", 
                        expr("size(pagesInSession) = 1 AND sessionDurationSeconds < 10"))
                .groupBy(col("pageUrl"), col("windowStart"))
                .agg(
                        (sum(when(col("isBounce"), 1).otherwise(0))
                                .divide(count("*")))
                                .alias("bounceRate")
                );
        
        // Join bounce rate with page metrics
        Dataset<Row> result = pageMetrics
                .join(sessionBounces, 
                        pageMetrics.col("pageUrl").equalTo(sessionBounces.col("pageUrl"))
                                .and(pageMetrics.col("windowStart")
                                        .equalTo(sessionBounces.col("windowStart"))),
                        "left")
                .select(
                        pageMetrics.col("pageUrl"),
                        pageMetrics.col("windowStart"),
                        pageMetrics.col("windowEnd"),
                        pageMetrics.col("totalViews"),
                        pageMetrics.col("uniqueVisitors"),
                        pageMetrics.col("clickCount"),
                        pageMetrics.col("avgScrollDepth"),
                        coalesce(sessionBounces.col("bounceRate"), lit(0.0))
                                .alias("bounceRate")
                );
        
        // Add composite key for MongoDB upsert
        return result.withColumn("_compositeKey",
                concat(col("pageUrl"), lit("_"), col("windowStart")));
    }
}
