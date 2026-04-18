package com.clickstream.etl.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

/**
 * Transforms raw clickstream events into user journey maps.
 * 
 * <p>Produces records with structure:
 * <pre>
 * {
 *   userId: string,
 *   sessionId: string,
 *   windowStart: timestamp,
 *   windowEnd: timestamp,
 *   orderedPages: array[{
 *     pageUrl: string,
 *     timestamp: long,
 *     clicksOnPage: int
 *   }],
 *   totalSessionDuration: long
 * }
 * </pre>
 * 
 * <p>Session definition: Events grouped by 30-minute session window.
 * Only PAGE_VIEW events contribute to journey tracking.
 */
@Component
public class UserJourneyBuilder {

    /**
     * Transforms raw events into user journey maps.
     * 
     * @param rawEvents DataFrame with watermarked timestamp column
     * @param sessionGapMinutes session window gap in minutes
     * @return journey DataFrame ready for MongoDB sink
     */
    public Dataset<Row> build(Dataset<Row> rawEvents, int sessionGapMinutes) {
        String sessionGap = sessionGapMinutes + " minutes";
        
        // Filter to PAGE_VIEW events only for journey tracking
        Dataset<Row> pageViews = rawEvents
                .filter(col("eventType").equalTo("PAGE_VIEW"));
        
        // For each page view, count associated clicks (clicks within same session/page)
        Dataset<Row> clickCounts = rawEvents
                .filter(col("eventType").equalTo("CLICK"))
                .groupBy(col("sessionId"), col("pageUrl"))
                .agg(count("*").alias("clicksOnPage"));
        
        // Join click counts with page views
        Dataset<Row> enrichedPageViews = pageViews
                .join(clickCounts,
                        pageViews.col("sessionId").equalTo(clickCounts.col("sessionId"))
                                .and(pageViews.col("pageUrl")
                                        .equalTo(clickCounts.col("pageUrl"))),
                        "left")
                .select(
                        pageViews.col("userId"),
                        pageViews.col("sessionId"),
                        pageViews.col("timestamp"),
                        pageViews.col("pageUrl"),
                        coalesce(clickCounts.col("clicksOnPage"), lit(0))
                                .alias("clicksOnPage")
                );
        
        // Build journey with session windows
        Dataset<Row> journeys = enrichedPageViews
                .groupBy(
                        session_window(col("timestamp"), sessionGap),
                        col("userId"),
                        col("sessionId"))
                .agg(
                        // Window bounds
                        col("session_window.start").alias("windowStart"),
                        col("session_window.end").alias("windowEnd"),
                        
                        // Ordered page sequence (collect and sort within group)
                        collect_list(struct(col("pageUrl"), col("timestamp"), col("clicksOnPage")))
                                .alias("orderedPages"),
                        
                        // Total session duration
                        expr("max(timestamp) - min(timestamp)")
                                .alias("totalSessionDuration")
                )
                .drop("session_window");
        
        // Sort orderedPages array by timestamp (ensure ordering)
        return journeys
                .withColumn("orderedPages",
                        expr("array_sort(orderedPages, " +
                             "(left, right) -> CASE WHEN left.timestamp < right.timestamp " +
                             "THEN -1 WHEN left.timestamp > right.timestamp THEN 1 ELSE 0 END)"))
                .withColumn("_compositeKey",
                        concat(col("userId"), lit("_"), col("sessionId")));
    }
}
