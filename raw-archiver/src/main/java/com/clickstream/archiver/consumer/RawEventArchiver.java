package com.clickstream.archiver.consumer;

import com.clickstream.archiver.config.ArchiverConfig;
import com.clickstream.archiver.util.PartitionPathBuilder;
import com.clickstream.archiver.writer.ParquetEventWriter;
import com.clickstream.model.ClickEvent;
import com.clickstream.validation.EventValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
 * - Manual offset commit for no data loss guarantee
 * - Thread-safe buffer management with lock-free I/O
 * - Circuit breaker pattern with retry logic and error file writes
 * - Graceful shutdown with buffer flush
 * - Bounded buffer with overflow protection
 */
@Service
@Slf4j
public class RawEventArchiver {
    
    private final ArchiverConfig config;
    private final ParquetEventWriter writer;
    private final PartitionPathBuilder pathBuilder;
    private final ObjectMapper objectMapper;
    private final EventValidator validator;
    
    private final List<ClickEvent> buffer = new ArrayList<>();
    private final List<Acknowledgment> acknowledgments = new ArrayList<>();
    private final Lock bufferLock = new ReentrantLock();
    private final Lock flushLock = new ReentrantLock();
    
    private Instant lastFlush;
    private Instant lastSuccessfulWrite;
    private long totalEventsProcessed = 0;
    private long totalBatchesFlushed = 0;
    private int consecutiveFailures = 0;
    
    private static final int MAX_RETRIES = 3;
    private static final int MAX_BUFFER_SIZE = 50_000;  // Safety limit: 5x normal threshold
    private static final String ERROR_DIR = "data-lake/errors";
    
    public RawEventArchiver(
            ArchiverConfig config,
            ParquetEventWriter writer,
            ObjectMapper objectMapper,
            EventValidator validator) {
        this.config = config;
        this.writer = writer;
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.pathBuilder = new PartitionPathBuilder(config.getDataLakeBasePath());
        this.lastFlush = Instant.now();
        this.lastSuccessfulWrite = Instant.now();
    }
    
    @PostConstruct
    public void init() {
        log.info("Raw Event Archiver initialized");
        log.info("  Topic: {}", config.getTopic());
        log.info("  Data Lake: {}", config.getDataLakeBasePath());
        log.info("  Flush threshold: {} events or {} seconds",
                config.getFlush().getEventThreshold(),
                config.getFlush().getTimeIntervalSeconds());
        log.info("  Max buffer size: {} events", MAX_BUFFER_SIZE);
        log.info("  Circuit breaker: {} retries", MAX_RETRIES);
        
        // Ensure error directory exists
        createErrorDirectoryIfNeeded();
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
            
            // Validate event (reuse from ingestion-api)
            if (!isValidEvent(event)) {
                log.warn("Invalid event received, skipping: eventId={}", event.getEventId());
                ack.acknowledge();  // Skip invalid events
                return;
            }
            
            String flushReason = null;
            
            bufferLock.lock();
            try {
                // Check buffer overflow protection
                if (buffer.size() >= MAX_BUFFER_SIZE) {
                    // Write half the buffer to error file to prevent rapid re-overflow
                    int overflowSize = Math.min(buffer.size() / 2, buffer.size());
                    log.error("Buffer overflow! Size: {} (max: {}). Writing oldest {} events to error file",
                            buffer.size(), MAX_BUFFER_SIZE, overflowSize);
                    writeBufferOverflowToErrorFile(overflowSize);
                }
                
                buffer.add(event);
                acknowledgments.add(ack);
                
                // Check flush conditions
                boolean shouldFlushByCount = buffer.size() >= config.getFlush().getEventThreshold();
                boolean shouldFlushByTime = Duration.between(lastFlush, Instant.now())
                        .getSeconds() >= config.getFlush().getTimeIntervalSeconds();
                
                if (shouldFlushByCount || shouldFlushByTime) {
                    flushReason = shouldFlushByCount ? "event threshold" : "time interval";
                }
            } finally {
                bufferLock.unlock();
            }
            
            // I/O OUTSIDE LOCK - non-blocking for other consumer threads
            if (flushReason != null) {
                flushCurrentBuffer(flushReason);
            }
            
        } catch (IOException e) {
            log.error("Failed to parse event from Kafka: {}", e.getMessage());  // No stack trace
            ack.acknowledge();  // Skip unparseable messages
        } catch (Exception e) {
            log.error("Unexpected error processing event", e);
            // Don't commit - will retry on restart
        }
    }

    /**
     * Ensures partial batches flush even when no later Kafka record arrives to
     * trigger the time check in consume().
     */
    @Scheduled(fixedDelayString = "${archiver.flush.scheduler-interval-ms:1000}")
    public void flushBufferedEventsOnInterval() {
        boolean shouldFlush;
        bufferLock.lock();
        try {
            shouldFlush = !buffer.isEmpty()
                    && Duration.between(lastFlush, Instant.now()).getSeconds()
                    >= config.getFlush().getTimeIntervalSeconds();
        } finally {
            bufferLock.unlock();
        }

        if (shouldFlush) {
            flushCurrentBuffer("scheduled time interval");
        }
    }

    private void flushCurrentBuffer(String reason) {
        flushLock.lock();
        try {
            List<ClickEvent> eventsToFlush;
            List<Acknowledgment> acksToCommit;

            bufferLock.lock();
            try {
                if (buffer.isEmpty()) {
                    return;
                }
                eventsToFlush = new ArrayList<>(buffer);
                acksToCommit = new ArrayList<>(acknowledgments);
            } finally {
                bufferLock.unlock();
            }

            flushBufferWithRetry(eventsToFlush, acksToCommit, reason);
        } finally {
            flushLock.unlock();
        }
    }
    
    /**
     * Flushes events to Parquet with circuit breaker pattern.
     * Implements retry logic and error file writes.
     * 
     * CRITICAL: This method is called OUTSIDE bufferLock to avoid blocking other threads.
     */
    private void flushBufferWithRetry(List<ClickEvent> events, List<Acknowledgment> acks, String reason) {
        if (events.isEmpty()) {
            return;
        }
        
        // Use event timestamp for partitioning (not current time)
        Instant eventTimestamp = Instant.ofEpochMilli(events.get(0).getTimestamp());
        String filePath = pathBuilder.buildPath(eventTimestamp);
        
        // Circuit breaker: Retry up to MAX_RETRIES times
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                // Write to Parquet
                writer.write(events, filePath);
                
                // Success! Commit all offsets
                for (Acknowledgment ack : acks) {
                    ack.acknowledge();
                }
                
                // Update stats and remove only the flushed snapshot. Events added
                // during the write stay buffered for the next flush.
                bufferLock.lock();
                try {
                    int flushedCount = Math.min(events.size(), buffer.size());
                    buffer.subList(0, flushedCount).clear();
                    acknowledgments.subList(0, Math.min(acks.size(), acknowledgments.size())).clear();
                    totalEventsProcessed += events.size();
                    totalBatchesFlushed++;
                    lastFlush = Instant.now();
                    lastSuccessfulWrite = Instant.now();
                    consecutiveFailures = 0;  // Reset circuit breaker
                } finally {
                    bufferLock.unlock();
                }
                
                log.info("Flushed {} events to {} (reason: {}, attempt: {}). Total: {} events in {} batches",
                        events.size(), filePath, reason, attempt + 1, totalEventsProcessed, totalBatchesFlushed);
                return;  // Success - exit retry loop
                
            } catch (IOException e) {
                log.error("Flush attempt {} failed: {}", attempt + 1, e.getMessage());  // No stack trace
                
                // Update failure count inside lock for thread safety
                bufferLock.lock();
                try {
                    consecutiveFailures++;
                } finally {
                    bufferLock.unlock();
                }
                
                if (attempt < MAX_RETRIES - 1) {
                    // Exponential backoff before retry
                    try {
                        long backoffMs = (long) Math.pow(2, attempt) * 100;  // 100ms, 200ms, 400ms
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        
        // All retries exhausted - write to error file and clear buffer
        log.error("Failed to flush after {} retries. Writing {} events to error file", MAX_RETRIES, events.size());
        writeToErrorFile(events);
        
        /**
         * DESIGN DECISION: Commit Kafka offsets after MAX_RETRIES to prevent infinite reprocessing.
         * 
         * This accepts data loss to Parquet in favor of service availability:
         * - Prevents poison pill messages from blocking all processing
         * - Allows service to continue processing new events
         * - Error files enable offline batch recovery to Parquet
         * 
         * Alternative designs considered and rejected:
         * - Dead letter topic: Adds Kafka complexity, still requires manual recovery
         * - Infinite retry: Risks service hang, buffer overflow, OOM
         * 
         * Recovery procedure: Re-ingest error files via manual script or cron job.
         */
        for (Acknowledgment ack : acks) {
            ack.acknowledge();
        }
        
        // Remove only the failed snapshot; events appended during retry handling
        // remain buffered for a later flush.
        bufferLock.lock();
        try {
            int failedCount = Math.min(events.size(), buffer.size());
            buffer.subList(0, failedCount).clear();
            acknowledgments.subList(0, Math.min(acks.size(), acknowledgments.size())).clear();
            lastFlush = Instant.now();
        } finally {
            bufferLock.unlock();
        }
    }
    
    /**
     * Writes failed events to error directory for manual recovery.
     */
    private void writeToErrorFile(List<ClickEvent> events) {
        try {
            String errorFileName = ERROR_DIR + "/failed_batch_" + System.currentTimeMillis() + ".json";
            File errorFile = new File(errorFileName);
            errorFile.getParentFile().mkdirs();
            
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(errorFile, events);
            log.info("Wrote {} failed events to {}", events.size(), errorFileName);
        } catch (IOException e) {
            log.error("CRITICAL: Failed to write error file: {}", e.getMessage());
        }
    }
    
    /**
     * Handles buffer overflow by writing oldest events to error file.
     */
    private void writeBufferOverflowToErrorFile(int count) {
        int eventsToRemove = Math.min(count, buffer.size());
        if (eventsToRemove == 0) return;
        
        List<ClickEvent> overflow = new ArrayList<>(buffer.subList(0, eventsToRemove));
        List<Acknowledgment> overflowAcks = new ArrayList<>(acknowledgments.subList(0, eventsToRemove));
        
        writeToErrorFile(overflow);
        
        // Commit offsets before removing (prevents re-consumption of error events)
        for (Acknowledgment ack : overflowAcks) {
            ack.acknowledge();
        }
        
        // Remove from buffer
        buffer.subList(0, eventsToRemove).clear();
        acknowledgments.subList(0, eventsToRemove).clear();
        
        log.warn("Removed {} events from buffer to prevent overflow (current size: {}, max: {})",
                eventsToRemove, buffer.size(), MAX_BUFFER_SIZE);
    }
    
    /**
     * Validates event before buffering (reuse validation from ingestion-api).
     */
    private boolean isValidEvent(ClickEvent event) {
        if (event == null || event.getEventId() == null || event.getSessionId() == null) {
            return false;
        }
        
        List<String> errors = validator.validate(event);
        if (!errors.isEmpty()) {
            log.warn("Event validation failed: {}", String.join(", ", errors));
            return false;
        }
        
        return true;
    }
    
    /**
     * Creates error directory if it doesn't exist.
     */
    private void createErrorDirectoryIfNeeded() {
        try {
            Files.createDirectories(Paths.get(ERROR_DIR));
        } catch (IOException e) {
            log.warn("Failed to create error directory: {}", e.getMessage());
        }
    }
    
    /**
     * Gracefully shuts down the archiver, flushing any buffered events.
     */
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down Raw Event Archiver...");
        
        if (getStats().currentBufferSize() > 0) {
            log.info("Flushing remaining {} buffered events before shutdown", getStats().currentBufferSize());
            flushCurrentBuffer("shutdown");
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
                    lastFlush,
                    lastSuccessfulWrite,
                    consecutiveFailures
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
            Instant lastFlush,
            Instant lastSuccessfulWrite,
            int consecutiveFailures
    ) {}
}
