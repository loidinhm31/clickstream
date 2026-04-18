package com.clickstream.realtime.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Kafka Consumer Configuration Validation.
 * 
 * <p>Validates critical Kafka consumer settings at application startup.
 * Fails fast if configuration is incorrect to prevent runtime issues.
 * 
 * <p>Critical validations:
 * - Ack mode MUST be "batch" for efficient offset management
 * - Consumer group ID must be set
 * - Bootstrap servers must be configured
 */
@Configuration
public class KafkaConfigValidator {

    @Value("${spring.kafka.listener.ack-mode:manual}")
    private String ackMode;

    @Value("${spring.kafka.consumer.group-id:}")
    private String groupId;

    @Value("${spring.kafka.bootstrap-servers:}")
    private String bootstrapServers;

    @Bean
    public ApplicationListener<ApplicationReadyEvent> kafkaConfigValidation() {
        return event -> {
            // Validate ack mode
            if (!"batch".equalsIgnoreCase(ackMode)) {
                throw new IllegalStateException(
                        "Kafka ack-mode MUST be 'batch' for real-time analytics. Current: " + ackMode);
            }

            // Validate consumer group ID
            if (groupId == null || groupId.isEmpty()) {
                throw new IllegalStateException(
                        "Kafka consumer.group-id MUST be configured");
            }

            // Validate bootstrap servers
            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                throw new IllegalStateException(
                        "Kafka bootstrap-servers MUST be configured");
            }

            // All validations passed
            org.slf4j.LoggerFactory.getLogger(KafkaConfigValidator.class)
                    .info("Kafka configuration validated: group={}, ackMode={}, servers={}",
                            groupId, ackMode, bootstrapServers);
        };
    }
}
