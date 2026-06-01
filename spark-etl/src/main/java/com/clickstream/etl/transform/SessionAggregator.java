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
    public Dataset<Row> aggregate(Dataset<Row> rawEvents, String sessionGap) {
        // Create struct with timestamp and pageUrl for ordering
        Dataset<Row> eventsWithStruct = rawEvents
                .withColumn("pageInfo", struct(col("timestamp"), col("pageUrl")));
        
        // Aggregate by session window
        Dataset<Row> aggregated = eventsWithStruct
                .groupBy(
                        session_window(col("timestamp"), sessionGap),
                        col("sessionId"),
                        col("userId"))
                .agg(
                        // Window bounds
                        col("session_window.start").alias("windowStart"),
                        col("session_window.end").alias("windowEnd"),
                        
                        // Session duration in milliseconds
                        expr("(unix_timestamp(max(timestamp)) - unix_timestamp(min(timestamp))) * 1000")
                                .alias("durationMs"),
                        
                        // Event counts by type
                        count(when(col("eventType").equalTo("PAGE_VIEW"), 1))
                                .alias("pageViewCount"),
                        count(when(col("eventType").equalTo("CLICK"), 1))
                                .alias("clickCount"),
                        count(when(col("eventType").equalTo("SCROLL"), 1))
                                .alias("scrollEvents"),
                        
                        // Unique pages visited
                        collect_set("pageUrl").alias("uniquePages"),
                        
                        // Collect all page info structs for sorting
                        collect_list("pageInfo").alias("pageInfoList")
                )
                .drop("session_window")
                
                // Sort page info by timestamp and extract entry/exit pages
                .withColumn("sortedPages", expr("array_sort(pageInfoList, (left, right) -> CASE WHEN left.timestamp < right.timestamp THEN -1 WHEN left.timestamp > right.timestamp THEN 1 ELSE 0 END)"))
                .withColumn("entryPage", expr("sortedPages[0].pageUrl"))
                .withColumn("exitPage", expr("sortedPages[size(sortedPages)-1].pageUrl"))
                .drop("pageInfoList", "sortedPages");
        
        // Calculate bounce rate (single page AND < 10 seconds)
        return aggregated
                .withColumn("bounced",
                        expr("size(uniquePages) = 1 AND durationMs < 10000"))
                .withColumn("_compositeKey",
                        concat(col("sessionId"), lit("_"), col("windowStart")));
    }
}
