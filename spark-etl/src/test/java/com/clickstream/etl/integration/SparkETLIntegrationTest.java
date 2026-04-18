package com.clickstream.etl.integration;

import com.clickstream.etl.config.MongoConfig;
import com.clickstream.etl.config.SparkConfig;
import com.mongodb.client.MongoClient;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for Spark ETL pipeline.
 * 
 * <p>Tests Spring Boot context loading and bean configuration.
 * Full end-to-end testing with Kafka + MongoDB requires Docker containers
 * and is better suited for separate E2E test suite.
 */
@SpringBootTest
@ActiveProfiles("test")
class SparkETLIntegrationTest {
    
    @Autowired(required = false)
    private SparkSession sparkSession;
    
    @Autowired(required = false)
    private MongoClient mongoClient;
    
    @Test
    @DisplayName("Should load Spring Boot application context")
    void contextLoads() {
        // If context loads successfully, this test passes
        assertTrue(true, "Application context loaded");
    }
    
    @Test
    @DisplayName("Should create SparkSession bean")
    void sparkSessionBeanCreated() {
        assertNotNull(sparkSession, "SparkSession bean should be created");
        assertEquals("clickstream-etl-test", sparkSession.sparkContext().appName(),
                "Spark app name should match configuration");
    }
    
    @Test
    @DisplayName("Should create MongoClient bean")
    void mongoClientBeanCreated() {
        assertNotNull(mongoClient, "MongoClient bean should be created");
    }
    
    @Test
    @DisplayName("SparkSession should be properly configured")
    void sparkSessionConfiguration() {
        assertNotNull(sparkSession);
        
        String master = sparkSession.sparkContext().master();
        assertTrue(master.startsWith("local"), 
                "Should use local master for tests");
    }
}
