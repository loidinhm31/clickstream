package com.clickstream.etl.config;

import com.clickstream.etl.config.StreamingQueryMonitor;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

/**
 * Spark configuration for embedded SparkSession.
 * 
 * <p>Creates a SparkSession bean managed by Spring Boot. This approach
 * is suitable for development and testing. For production cluster deployment,
 * use spark-submit without the 'master' config.
 * 
 * <p>Important: Requires JVM flag: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
 * (handled in spring-boot-maven-plugin configuration)
 */
@Configuration
@Validated
public class SparkConfig {

    @NotBlank
    @Value("${spark.app-name}")
    private String appName;

    @NotBlank
    @Value("${spark.master}")
    private String master;

    @Positive
    @Value("${spark.config.sql.session.window.buffer.in.memory.threshold}")
    private int sessionWindowBufferThreshold;

    @Value("${spark.config.sql.streaming.forceDeleteTempCheckpointLocation}")
    private boolean forceDeleteTempCheckpoint;

    @Value("${spark.config.executor.memory}")
    private String executorMemory;

    @Value("${spark.config.driver.memory}")
    private String driverMemory;

    private final StreamingQueryMonitor queryMonitor;

    public SparkConfig(StreamingQueryMonitor queryMonitor) {
        this.queryMonitor = queryMonitor;
    }

    /**
     * Creates and configures SparkSession bean.
     * 
     * @return configured SparkSession instance
     */
    @Bean(destroyMethod = "stop")
    public SparkSession sparkSession() {
        SparkSession spark = SparkSession.builder()
                .appName(appName)
                .master(master)
                .config("spark.sql.session.window.buffer.in.memory.threshold", 
                        sessionWindowBufferThreshold)
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", 
                        forceDeleteTempCheckpoint)
                .config("spark.executor.memory", executorMemory)
                .config("spark.driver.memory", driverMemory)
                // Disable Spark UI to avoid javax.servlet/jakarta.servlet conflict with Spring Boot 3.x
                .config("spark.ui.enabled", "false")
                // Suppress excessive Spark logging
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate();
        
        // Register streaming query monitor
        spark.streams().addListener(queryMonitor);
        
        return spark;
    }
}
