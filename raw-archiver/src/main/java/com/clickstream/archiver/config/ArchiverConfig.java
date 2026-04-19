package com.clickstream.archiver.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties for the archiver service.
 */
@Configuration
@ConfigurationProperties(prefix = "archiver")
@Getter
@Setter
public class ArchiverConfig {
    
    private String topic = "clickstream-events";
    private String dataLakeBasePath = "./data-lake";
    
    private FlushConfig flush = new FlushConfig();
    private ParquetConfig parquet = new ParquetConfig();
    
    @Getter
    @Setter
    public static class FlushConfig {
        private int eventThreshold = 10000;
        private int timeIntervalSeconds = 60;
    }
    
    @Getter
    @Setter
    public static class ParquetConfig {
        private String compression = "SNAPPY";
        private int pageSize = 1048576;  // 1MB
        private int rowGroupSize = 134217728;  // 128MB
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
