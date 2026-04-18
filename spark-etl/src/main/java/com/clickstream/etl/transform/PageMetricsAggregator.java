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
                                .alias("avgScrollDepth"),
                        
                        // Placeholder for bounce rate (requires stateful processing)
                        // Will be implemented in future phase with stream-stream join watermarks
                        lit(0.0).alias("bounceRate")
                )
                .drop("window");
        
        // Add composite key for MongoDB upsert
        return pageMetrics.withColumn("_compositeKey",
                concat(col("pageUrl"), lit("_"), col("windowStart")));
    }
}
