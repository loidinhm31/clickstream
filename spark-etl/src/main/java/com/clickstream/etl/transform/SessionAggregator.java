package com.clickstream.etl.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

/**
 * Transforms raw clickstream events into session-level aggregates.
 * 
 * <p>Produces records with structure:
 * <pre>
 * {
 *   sessionId: string,
 *   userId: string,
 *   windowStart: timestamp,
 *   windowEnd: timestamp,
 *   durationMs: long,
 *   pageViewCount: long,
 *   clickCount: long,
 *   scrollEvents: long,
 *   uniquePages: array[string],
 *   entryPage: string,
 *   exitPage: string,
 *   bounced: boolean
 * }
 * </pre>
 * 
 * <p>Session definition: Events grouped by 30-minute session window
 * (gap-based window - if no events for 30 minutes, session ends).
 */
@Component
public class SessionAggregator {

    /**
     * Transforms raw events into session aggregates.
     * 
     * @param rawEvents DataFrame with watermarked timestamp column
     * @param sessionGapMinutes session window gap in minutes
     * @return aggregated DataFrame ready for MongoDB sink
     */
    public Dataset<Row> aggregate(Dataset<Row> rawEvents, int sessionGapMinutes) {
        // Define session window (gap-based windowing)
        String sessionGap = sessionGapMinutes + " minutes";
        
        // Window spec for ordering within each session
        var sessionWindow = Window
                .partitionBy("sessionId", "userId")
                .orderBy("timestamp");
        
        // First, add entry/exit page information using window functions
        Dataset<Row> enriched = rawEvents
                .withColumn("entryPage", 
                        first("pageUrl").over(sessionWindow))
                .withColumn("exitPage", 
                        last("pageUrl").over(sessionWindow));
        
        // Aggregate by session window
        Dataset<Row> aggregated = enriched
                .groupBy(
                        session_window(col("timestamp"), sessionGap),
                        col("sessionId"),
                        col("userId"))
                .agg(
                        // Window bounds
                        col("session_window.start").alias("windowStart"),
                        col("session_window.end").alias("windowEnd"),
                        
                        // Session duration
                        expr("max(timestamp) - min(timestamp)").alias("durationMs"),
                        
                        // Event counts by type
                        count(when(col("eventType").equalTo("PAGE_VIEW"), 1))
                                .alias("pageViewCount"),
                        count(when(col("eventType").equalTo("CLICK"), 1))
                                .alias("clickCount"),
                        count(when(col("eventType").equalTo("SCROLL"), 1))
                                .alias("scrollEvents"),
                        
                        // Unique pages visited
                        collect_set("pageUrl").alias("uniquePages"),
                        
                        // Entry and exit pages (first/last by time)
                        first("entryPage").alias("entryPage"),
                        first("exitPage").alias("exitPage")
                )
                .drop("session_window");
        
        // Calculate bounce rate (single page AND < 10 seconds)
        return aggregated
                .withColumn("bounced",
                        expr("size(uniquePages) = 1 AND durationMs < 10000"))
                .withColumn("_compositeKey",
                        concat(col("sessionId"), lit("_"), col("windowStart")));
    }
}
