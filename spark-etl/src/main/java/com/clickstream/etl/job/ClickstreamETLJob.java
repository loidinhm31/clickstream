package com.clickstream.etl.job;

import com.clickstream.etl.schema.EventSchema;
import com.clickstream.etl.sink.MongoForeachBatchWriter;
import com.clickstream.etl.transform.PageMetricsAggregator;
import com.clickstream.etl.transform.SessionAggregator;
import com.clickstream.etl.transform.UserJourneyBuilder;
import jakarta.annotation.PreDestroy;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

/**
 * Main ETL job orchestrator.
 * 
 * <p>Consumes clickstream events from Kafka, applies transformations,
 * and sinks results to MongoDB via three parallel streaming queries:
 * <ol>
 *   <li>Session aggregates (30-min session windows)</li>
 *   <li>Page-level metrics (5-min tumbling windows)</li>
 *   <li>User journey maps (session-based)</li>
 * </ol>
 * 
 * <p>Starts automatically on Spring Boot application startup.
 */
@Component
public class ClickstreamETLJob implements CommandLineRunner {
    
    private static final Logger logger = LoggerFactory.getLogger(ClickstreamETLJob.class);
    
    private final SparkSession spark;
    private final MongoForeachBatchWriter mongoWriter;
    private final SessionAggregator sessionAggregator;
    private final PageMetricsAggregator pageMetricsAggregator;
    private final UserJourneyBuilder userJourneyBuilder;
    
    // Streaming queries for graceful shutdown
    private StreamingQuery sessionQuery;
    private StreamingQuery pageMetricsQuery;
    private StreamingQuery journeyQuery;
    
    @Value("${kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;
    
    @Value("${kafka.topic}")
    private String kafkaTopic;
    
    @Value("${kafka.starting-offsets}")
    private String startingOffsets;
    
    @Value("${spark.checkpoint-location}")
    private String checkpointLocation;
    
    @Value("${streaming.trigger.processing-time}")
    private int processingTimeSeconds;
    
    @Value("${streaming.watermark.delay}")
    private String watermarkDelay;
    
    @Value("${streaming.session.gap}")
    private String sessionGap;
    
    @Value("${streaming.page-metrics.window}")
    private String pageMetricsWindow;
    
    @Value("${mongodb.collections.session-aggregates}")
    private String sessionAggregatesCollection;
    
    @Value("${mongodb.collections.page-metrics}")
    private String pageMetricsCollection;
    
    @Value("${mongodb.collections.user-journeys}")
    private String userJourneysCollection;
    
    public ClickstreamETLJob(
            SparkSession spark,
            MongoForeachBatchWriter mongoWriter,
            SessionAggregator sessionAggregator,
            PageMetricsAggregator pageMetricsAggregator,
            UserJourneyBuilder userJourneyBuilder) {
        this.spark = spark;
        this.mongoWriter = mongoWriter;
        this.sessionAggregator = sessionAggregator;
        this.pageMetricsAggregator = pageMetricsAggregator;
        this.userJourneyBuilder = userJourneyBuilder;
    }
    
    @Override
    public void run(String... args) throws Exception {
        logger.info("Starting Clickstream ETL Job");
        logger.info("Kafka: {} | Topic: {}", kafkaBootstrapServers, kafkaTopic);
        logger.info("MongoDB collections: {}, {}, {}", 
                sessionAggregatesCollection, pageMetricsCollection, userJourneysCollection);
        
        // Create raw event stream from Kafka
        Dataset<Row> rawStream = createKafkaStream();
        
        // Start three parallel streaming queries
        sessionQuery = startSessionAggregation(rawStream);
        pageMetricsQuery = startPageMetricsAggregation(rawStream);
        journeyQuery = startUserJourneyBuilding(rawStream);
        
        logger.info("All streaming queries started successfully");
        logger.info("Session query: {}", sessionQuery.id());
        logger.info("Page metrics query: {}", pageMetricsQuery.id());
        logger.info("Journey query: {}", journeyQuery.id());
        
        // Wait for any query to terminate (keeps application alive)
        spark.streams().awaitAnyTermination();
    }
    
    /**
     * Gracefully stops all streaming queries on application shutdown.
     */
    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down streaming queries gracefully...");
        
        try {
            if (sessionQuery != null && sessionQuery.isActive()) {
                sessionQuery.stop();
                logger.info("Session query stopped");
            }
            if (pageMetricsQuery != null && pageMetricsQuery.isActive()) {
                pageMetricsQuery.stop();
                logger.info("Page metrics query stopped");
            }
            if (journeyQuery != null && journeyQuery.isActive()) {
                journeyQuery.stop();
                logger.info("Journey query stopped");
            }
            logger.info("All streaming queries stopped successfully");
        } catch (Exception e) {
            logger.error("Error during streaming query shutdown", e);
        }
    }
    
    /**
     * Creates Kafka streaming source with JSON parsing and watermarking.
     * 
     * @return parsed and watermarked event DataFrame
     */
    private Dataset<Row> createKafkaStream() {
        logger.info("Creating Kafka stream from topic: {}", kafkaTopic);
        
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("subscribe", kafkaTopic)
                .option("startingOffsets", startingOffsets)
                .option("failOnDataLoss", "false")  // Don't fail if topic is recreated
                .load()
                // Extract value as string
                .selectExpr("CAST(value AS STRING) as json")
                // Parse JSON to struct
                .select(from_json(col("json"), EventSchema.clickEventSchema())
                        .alias("event"))
                .select("event.*")
                // Convert timestamp from epoch millis to Spark timestamp
                .withColumn("timestamp", 
                        to_timestamp(from_unixtime(col("timestamp").divide(1000))))
                // Apply watermark for late event handling
                .withWatermark("timestamp", watermarkDelay);
    }
    
    /**
     * Starts session aggregation streaming query.
     */
    private StreamingQuery startSessionAggregation(Dataset<Row> rawStream) throws Exception {
        logger.info("Starting session aggregation query (gap: {})", sessionGap);
        
        Dataset<Row> sessionAggregates = sessionAggregator.aggregate(
                rawStream, sessionGap);
        
        return sessionAggregates.writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDf, batchId) -> 
                        mongoWriter.writeBatch(batchDf, batchId, sessionAggregatesCollection))
                .option("checkpointLocation", 
                        checkpointLocation + "/sessions")
                .trigger(Trigger.ProcessingTime(processingTimeSeconds + " seconds"))
                .start();
    }
    
    /**
     * Starts page metrics aggregation streaming query.
     */
    private StreamingQuery startPageMetricsAggregation(Dataset<Row> rawStream) throws Exception {
        logger.info("Starting page metrics query (window: {})", pageMetricsWindow);
        
        Dataset<Row> pageMetrics = pageMetricsAggregator.aggregate(
                rawStream, pageMetricsWindow);
        
        return pageMetrics.writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDf, batchId) -> 
                        mongoWriter.writeBatch(batchDf, batchId, pageMetricsCollection))
                .option("checkpointLocation", 
                        checkpointLocation + "/page-metrics")
                .trigger(Trigger.ProcessingTime(processingTimeSeconds + " seconds"))
                .start();
    }
    
    /**
     * Starts user journey building streaming query.
     */
    private StreamingQuery startUserJourneyBuilding(Dataset<Row> rawStream) throws Exception {
        logger.info("Starting user journey query (gap: {})", sessionGap);
        
        Dataset<Row> userJourneys = userJourneyBuilder.build(
                rawStream, sessionGap);
        
        return userJourneys.writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDf, batchId) -> 
                        mongoWriter.writeBatch(batchDf, batchId, userJourneysCollection))
                .option("checkpointLocation", 
                        checkpointLocation + "/user-journeys")
                .trigger(Trigger.ProcessingTime(processingTimeSeconds + " seconds"))
                .start();
    }
}
