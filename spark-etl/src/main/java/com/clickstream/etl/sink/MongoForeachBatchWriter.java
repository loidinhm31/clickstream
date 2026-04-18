package com.clickstream.etl.sink;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;

/**
 * MongoDB sink writer using foreachBatch pattern.
 * 
 * <p>Provides control over write operations, enabling upsert semantics
 * based on composite keys. This approach is more reliable than
 * Mongo Spark Connector's streaming mode for production workloads.
 * 
 * <p>Upsert strategy: Uses _compositeKey field for conflict detection.
 * If a document with the same key exists, it's replaced; otherwise inserted.
 */
@Component
public class MongoForeachBatchWriter implements Serializable {
    
    private static final Logger logger = LoggerFactory.getLogger(MongoForeachBatchWriter.class);
    
    private final String mongoUri;
    private final String databaseName;
    
    public MongoForeachBatchWriter(
            @Value("${mongodb.uri}") String mongoUri,
            @Value("${mongodb.database}") String databaseName) {
        this.mongoUri = mongoUri;
        this.databaseName = databaseName;
    }
    
    /**
     * Writes a batch of rows to MongoDB collection with upsert.
     * 
     * @param batchDf DataFrame batch from streaming query
     * @param batchId unique batch identifier from Spark
     * @param collectionName target MongoDB collection
     */
    public void writeBatch(Dataset<Row> batchDf, long batchId, String collectionName) {
        long startTime = System.currentTimeMillis();
        
        logger.info("Writing batch {} to collection {}", batchId, collectionName);
        
        try {
            // Create MongoClient per batch (avoids serialization issues)
            try (MongoClient mongoClient = MongoClients.create(mongoUri)) {
                MongoCollection<Document> collection = mongoClient
                        .getDatabase(databaseName)
                        .getCollection(collectionName);
                
                long rowCount = 0;
                // Use local iterator to avoid serialization issues
                for (Row row : batchDf.toLocalIterator()) {
                    try {
                        // Convert Spark Row to BSON Document
                        Document doc = rowToDocument(row);
                        
                        // Extract composite key for upsert
                        Object compositeKey = doc.get("_compositeKey");
                        if (compositeKey == null) {
                            logger.warn("Row missing _compositeKey, inserting without upsert: {}", 
                                    row.json());
                            collection.insertOne(doc);
                        } else {
                            // Upsert with retry logic
                            retryUpsert(collection, compositeKey, doc, 3);
                        }
                        rowCount++;
                    } catch (Exception e) {
                        logger.error("Failed to write row to MongoDB after retries: {}", row.json(), e);
                        // Continue processing other rows
                    }
                }
                
                long duration = System.currentTimeMillis() - startTime;
                logger.info("Batch {} written to {} in {}ms ({} rows, {} rows/sec)",
                        batchId, collectionName, duration, rowCount,
                        rowCount * 1000 / Math.max(duration, 1));
            
        } catch (Exception e) {
            logger.error("Failed to write batch {} to collection {}", 
                    batchId, collectionName, e);
            throw new RuntimeException("MongoDB write failure", e);
        }
    }
    
    /**
     * Retries MongoDB upsert operation with exponential backoff.
     */
    private void retryUpsert(MongoCollection<Document> collection, Object compositeKey, 
                            Document doc, int maxRetries) throws Exception {
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                collection.replaceOne(
                        Filters.eq("_compositeKey", compositeKey),
                        doc,
                        new ReplaceOptions().upsert(true)
                );
                return;  // Success
            } catch (Exception e) {
                if (attempt == maxRetries - 1) {
                    throw e;  // Final retry failed
                }
                logger.warn("MongoDB write failed (attempt {}/{}), retrying...", 
                        attempt + 1, maxRetries);
                Thread.sleep(100 * (long) Math.pow(2, attempt));  // Exponential backoff
            }
        }
    }
    
    /**
     * Converts Spark Row to BSON Document.
     * 
     * @param row Spark Row from DataFrame
     * @return BSON Document ready for MongoDB
     */
    private Document rowToDocument(Row row) {
        // Use Spark's JSON serialization, then parse as BSON
        String json = row.json();
        return Document.parse(json);
    }
}
