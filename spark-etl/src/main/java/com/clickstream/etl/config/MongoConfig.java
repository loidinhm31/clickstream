package com.clickstream.etl.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * MongoDB configuration for ETL sink.
 * 
 * <p>Creates MongoClient bean for direct write operations via foreachBatch.
 * This gives more control than the Mongo Spark Connector's streaming mode.
 */
@Configuration
public class MongoConfig {

    @Value("${mongodb.uri}")
    private String mongoUri;

    /**
     * Creates MongoDB client bean.
     * 
     * @return configured MongoClient instance
     */
    @Bean(destroyMethod = "close")
    public MongoClient mongoClient() {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(mongoUri))
                .build();
        
        return MongoClients.create(settings);
    }
}
