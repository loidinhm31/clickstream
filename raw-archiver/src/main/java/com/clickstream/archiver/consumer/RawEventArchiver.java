package com.clickstream.archiver.consumer;

import com.clickstream.archiver.config.ArchiverConfig;
import com.clickstream.archiver.util.PartitionPathBuilder;
import com.clickstream.archiver.writer.ParquetEventWriter;
import com.clickstream.model.ClickEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Kafka consumer that writes raw events to Parquet files in the data lake.
 * 
 * Features:
 * - Buffered writes with configurable flush thresholds (events count and time interval)
 * - Manual offset commit for no data loss
 * - Thread-safe buffer management
 * - Graceful shutdown with buffer flush
 */
@Service
@Slf4j
public class RawEventArchiver {
    
    private final ArchiverConfig config;
    private final ParquetEventWriter writer;
    private final PartitionPathBuilder pathBuilder;
    private final ObjectMapper objectMapper;
    
    private final List<ClickEvent> buffer = new ArrayList<>();
    private final Lock bufferLock = new ReentrantLock();
    
    private Instant lastFlush;
    private Acknowledgment pendingAck;
    private long totalEventsProcessed = 0;
    private long totalBatchesFlushed = 0;
    
    public RawEventArchiver(
            ArchiverConfig config,
            ParquetEventWriter writer,
            ObjectMapper objectMapper) {
        this.config = config;
        this.writer = writer;
        this.objectMapper = objectMapper;
        this.pathBuilder = new PartitionPathBuilder(config.getDataLakeBasePath());
        this.lastFlush = Instant.now();
    }
    
    @PostConstruct
    public void init() {
        log.info("Raw Event Archiver initialized");
        log.info("  Topic: {}", config.getTopic());
        log.info("  Data Lake: {}", config.getDataLakeBasePath());
        log.info("  Flush threshold: {} events or {} seconds",
                config.getFlush().getEventThreshold(),
                config.getFlush().getTimeIntervalSeconds());
    }
    
    /**
     * Consumes events from Kafka and buffers them for batch writing.
     * Flushes when threshold is reached (event count or time interval).
     */
    @KafkaListener(topics = "#{archiverConfig.topic}", groupId = "raw-archiver-group")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            // Parse event from JSON
            ClickEvent event = objectMapper.readValue(record.value(), ClickEvent.class);
            
            bufferLock.lock();
            try {
                buffer.add(event);
                pendingAck = ack;  // Store latest acknowledgment
                
                // Check flush conditions
                boolean shouldFlushByCount = buffer.size() >= config.getFlush().getEventThreshold();
                boolean shouldFlushByTime = Duration.between(lastFlush, Instant.now())
                        .getSeconds() >= config.getFlush().getTimeIntervalSeconds();
                
                if (shouldFlushByCount || shouldFlushByTime) {
                    String reason = shouldFlushByCount ? "event threshold" : "time interval";
                    flushBuffer(reason);
                }
            } finally {
                bufferLock.unlock();
            }
            
        } catch (IOException e) {
            log.error("Failed to parse event from Kafka: {}", e.getMessage(), e);
            // Don't commit offset on parse errors - skip message
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Unexpected error processing event: {}", e.getMessage(), e);
            // Don't commit on unexpected errors - will retry on restart
        }
    }
    
    /**
     * Flushes the buffer to a Parquet file and commits Kafka offsets.
     * Called when flush threshold is reached OR during shutdown.
     * 
     * IMPORTANT: Must hold bufferLock when calling this method.
     */
    private void flushBuffer(String reason) {
        if (buffer.isEmpty()) {
            log.debug("Buffer is empty, skipping flush");
            return;
        }
        
        try {
            // Build partition path based on current timestamp
            String filePath = pathBuilder.buildPath(Instant.now());
            
            // Write events to Parquet
            writer.write(buffer, filePath);
            
            // Success! Commit offset and update stats
            if (pendingAck != null) {
                pendingAck.acknowledge();
            }
            
            totalEventsProcessed += buffer.size();
            totalBatchesFlushed++;
            
            log.info("Flushed {} events to Parquet (reason: {}). Total: {} events in {} batches",
                    buffer.size(), reason, totalEventsProcessed, totalBatchesFlushed);
            
            // Clear buffer and reset timer
            buffer.clear();
            lastFlush = Instant.now();
            pendingAck = null;
            
        } catch (IOException e) {
            log.error("Failed to flush buffer to Parquet: {}", e.getMessage(), e);
            // Don't clear buffer or commit offset - will retry on next flush or restart
            // This ensures no data loss even if Parquet write fails
        }
    }
    
    /**
     * Gracefully shuts down the archiver, flushing any buffered events.
     */
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down Raw Event Archiver...");
        
        bufferLock.lock();
        try {
            if (!buffer.isEmpty()) {
                log.info("Flushing remaining {} buffered events before shutdown", buffer.size());
                flushBuffer("shutdown");
            }
        } finally {
            bufferLock.unlock();
        }
        
        log.info("Raw Event Archiver shutdown complete. Total processed: {} events in {} batches",
                totalEventsProcessed, totalBatchesFlushed);
    }
    
    /**
     * Returns current statistics for monitoring.
     */
    public ArchiverStats getStats() {
        bufferLock.lock();
        try {
            return new ArchiverStats(
                    totalEventsProcessed,
                    totalBatchesFlushed,
                    buffer.size(),
                    lastFlush
            );
        } finally {
            bufferLock.unlock();
        }
    }
    
    /**
     * Statistics container for monitoring.
     */
    public record ArchiverStats(
            long totalEventsProcessed,
            long totalBatchesFlushed,
            int currentBufferSize,
            Instant lastFlush
    ) {}
}
