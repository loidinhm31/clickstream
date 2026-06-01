package com.clickstream.archiver;

import com.clickstream.validation.EventValidator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Raw Event Archiver Application
 * 
 * Consumes events from Kafka and writes them to Parquet files in a date-partitioned data lake.
 * 
 * Key features:
 * - Manual offset management for no data loss
 * - Buffered writes with configurable flush thresholds
 * - Hourly partitioned Parquet files with Snappy compression
 */
@SpringBootApplication
@EnableKafka
@EnableScheduling
public class ArchiverApplication {

    public static void main(String[] args) {
        SpringApplication.run(ArchiverApplication.class, args);
    }
}
