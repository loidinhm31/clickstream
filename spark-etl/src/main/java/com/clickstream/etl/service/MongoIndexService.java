package com.clickstream.etl.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * MongoDB index initialization service.
 * 
 * <p>Creates required indexes for ETL output collections on application startup.
 * Indexes are crucial for:
 * <ul>
 *   <li>Efficient upsert operations (composite key lookups)</li>
 *   <li>Query performance on aggregated data</li>
 *   <li>Data expiration (TTL on page metrics)</li>
 * </ul>
 * 
 * <p>Runs automatically when application is ready.
 */
@Service
public class MongoIndexService {
    
    private static final Logger logger = LoggerFactory.getLogger(MongoIndexService.class);
    
    private final MongoClient mongoClient;
    private final String databaseName;
    private final String sessionAggregatesCollection;
    private final String pageMetricsCollection;
    private final String userJourneysCollection;
    
    public MongoIndexService(
            MongoClient mongoClient,
            @Value("${mongodb.database}") String databaseName,
            @Value("${mongodb.collections.session-aggregates}") String sessionAggregatesCollection,
            @Value("${mongodb.collections.page-metrics}") String pageMetricsCollection,
            @Value("${mongodb.collections.user-journeys}") String userJourneysCollection) {
        this.mongoClient = mongoClient;
        this.databaseName = databaseName;
        this.sessionAggregatesCollection = sessionAggregatesCollection;
        this.pageMetricsCollection = pageMetricsCollection;
        this.userJourneysCollection = userJourneysCollection;
    }
    
    /**
     * Initializes all indexes when application is ready.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void initializeIndexes() {
        logger.info("Initializing MongoDB indexes for ETL collections");
        
        try {
            createSessionAggregatesIndexes();
            createPageMetricsIndexes();
            createUserJourneysIndexes();
            
            logger.info("MongoDB indexes created successfully");
        } catch (Exception e) {
            logger.error("Failed to create MongoDB indexes", e);
            throw new RuntimeException("Index creation failed", e);
        }
    }
    
    /**
     * Creates indexes for session_aggregates collection.
     * 
     * <ul>
     *   <li>_compositeKey (unique) - for upsert operations</li>
     *   <li>sessionId, windowStart - for querying by session</li>
     *   <li>userId, windowStart - for querying by user</li>
     * </ul>
     */
    private void createSessionAggregatesIndexes() {
        logger.info("Creating indexes for {}", sessionAggregatesCollection);
        
        MongoCollection<Document> collection = mongoClient
                .getDatabase(databaseName)
                .getCollection(sessionAggregatesCollection);
        
        // Composite key index (unique for upsert)
        collection.createIndex(
                Indexes.ascending("_compositeKey"),
                new IndexOptions().unique(true).name("idx_composite_key")
        );
        
        // Query optimization indexes
        collection.createIndex(
                Indexes.compoundIndex(
                        Indexes.ascending("sessionId"),
                        Indexes.descending("windowStart")
                ),
                new IndexOptions().name("idx_session_time")
        );
        
        collection.createIndex(
                Indexes.compoundIndex(
                        Indexes.ascending("userId"),
                        Indexes.descending("windowStart")
                ),
                new IndexOptions().name("idx_user_time")
        );
        
        logger.info("Session aggregates indexes created");
    }
    
    /**
     * Creates indexes for page_metrics collection.
     * 
     * <ul>
     *   <li>_compositeKey (unique) - for upsert operations</li>
     *   <li>pageUrl, windowStart - for querying by page</li>
     *   <li>windowStart (TTL 30 days) - auto-delete old metrics</li>
     * </ul>
     */
    private void createPageMetricsIndexes() {
        logger.info("Creating indexes for {}", pageMetricsCollection);
        
        MongoCollection<Document> collection = mongoClient
                .getDatabase(databaseName)
                .getCollection(pageMetricsCollection);
        
        // Composite key index (unique for upsert)
        collection.createIndex(
                Indexes.ascending("_compositeKey"),
                new IndexOptions().unique(true).name("idx_composite_key")
        );
        
        // Query optimization index
        collection.createIndex(
                Indexes.compoundIndex(
                        Indexes.ascending("pageUrl"),
                        Indexes.descending("windowStart")
                ),
                new IndexOptions().name("idx_page_time")
        );
        
        // TTL index - expire documents after 30 days
        collection.createIndex(
                Indexes.ascending("windowStart"),
                new IndexOptions()
                        .expireAfter(30L, TimeUnit.DAYS)
                        .name("idx_ttl")
        );
        
        logger.info("Page metrics indexes created (with 30-day TTL)");
    }
    
    /**
     * Creates indexes for user_journeys collection.
     * 
     * <ul>
     *   <li>_compositeKey (unique) - for upsert operations</li>
     *   <li>userId, sessionId - for querying by user/session</li>
     *   <li>windowStart - for time-based queries</li>
     * </ul>
     */
    private void createUserJourneysIndexes() {
        logger.info("Creating indexes for {}", userJourneysCollection);
        
        MongoCollection<Document> collection = mongoClient
                .getDatabase(databaseName)
                .getCollection(userJourneysCollection);
        
        // Composite key index (unique for upsert)
        collection.createIndex(
                Indexes.ascending("_compositeKey"),
                new IndexOptions().unique(true).name("idx_composite_key")
        );
        
        // Query optimization indexes
        collection.createIndex(
                Indexes.compoundIndex(
                        Indexes.ascending("userId"),
                        Indexes.ascending("sessionId")
                ),
                new IndexOptions().name("idx_user_session")
        );
        
        collection.createIndex(
                Indexes.descending("windowStart"),
                new IndexOptions().name("idx_window_start")
        );
        
        logger.info("User journeys indexes created");
    }
}
