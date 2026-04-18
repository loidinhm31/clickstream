package com.clickstream.realtime.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * Kafka Consumer Configuration for Real-time Analytics.
 * 
 * <p>Consumer Group: realtime-analytics-group
 * <p>Topic: clickstream-events
 * <p>Offset Strategy: latest (start from newest messages on first run)
 * <p>Ack Mode: batch (manual commit after batch processing)
 * 
 * <p>This consumer feeds the MetricsEngine ring buffer with incoming events.
 * The consumer group is independent from spark-etl and raw-archiver groups,
 * ensuring each service processes all events.
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    /**
     * Kafka consumer is configured via application.yml.
     * Spring Boot auto-configuration handles consumer factory setup.
     * 
     * <p>Key configurations:
     * <ul>
     *   <li>JsonDeserializer with trusted packages for ClickEvent deserialization</li>
     *   <li>auto-offset-reset: latest (avoid replaying historical events)</li>
     *   <li>batch ack mode (commit after MetricsEngine processes batch)</li>
     * </ul>
     */
}
