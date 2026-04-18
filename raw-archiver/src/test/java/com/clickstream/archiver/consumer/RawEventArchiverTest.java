package com.clickstream.archiver.consumer;

import com.clickstream.archiver.config.ArchiverConfig;
import com.clickstream.archiver.writer.ParquetEventWriter;
import com.clickstream.model.ClickEvent;
import com.clickstream.model.EventMetadata;
import com.clickstream.model.EventType;
import com.clickstream.validation.EventValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.support.Acknowledgment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RawEventArchiverTest {
    
    @Mock
    private Acknowledgment acknowledgment;
    
    private RawEventArchiver archiver;
    private ArchiverConfig config;
    private ParquetEventWriter writer;
    private ObjectMapper objectMapper;
    private EventValidator validator;
    
    @TempDir
    Path tempDir;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        config = new ArchiverConfig();
        config.setTopic("test-topic");
        config.setDataLakeBasePath(tempDir.toString());
        config.getFlush().setEventThreshold(10);
        config.getFlush().setTimeIntervalSeconds(60);
        
        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules(); // For Java 8 date/time support
        
        validator = new EventValidator();
        writer = new ParquetEventWriter(config);
        archiver = new RawEventArchiver(config, writer, objectMapper, validator);
    }
    
    @Test
    void consume_shouldBufferEventsBeforeThreshold() throws Exception {
        // Given: Events below threshold
        for (int i = 0; i < 5; i++) {
            ClickEvent event = createTestEvent(i);
            String json = objectMapper.writeValueAsString(event);
            ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, i, "key", json);
            
            // When
            archiver.consume(record, acknowledgment);
        }
        
        // Then: Should not commit yet (buffer not full)
        verify(acknowledgment, never()).acknowledge();
        
        RawEventArchiver.ArchiverStats stats = archiver.getStats();
        assertEquals(5, stats.currentBufferSize());
        assertEquals(0, stats.totalEventsProcessed());
    }
    
    @Test
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = "Hadoop winutils.exe not available on Windows - run in Docker/WSL")
    void consume_shouldFlushWhenThresholdReached() throws Exception {
        // Given: Events at threshold
        for (int i = 0; i < 10; i++) {
            ClickEvent event = createTestEvent(i);
            String json = objectMapper.writeValueAsString(event);
            ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, i, "key", json);
            
            // When
            archiver.consume(record, acknowledgment);
        }
        
        // Then: Should have flushed and committed
        verify(acknowledgment, atLeastOnce()).acknowledge();
        
        RawEventArchiver.ArchiverStats stats = archiver.getStats();
        assertEquals(0, stats.currentBufferSize());  // Buffer cleared after flush
        assertEquals(10, stats.totalEventsProcessed());
        assertEquals(1, stats.totalBatchesFlushed());
        
        // Verify Parquet file was created
        assertTrue(Files.list(tempDir).anyMatch(p -> p.toString().endsWith(".snappy.parquet")));
    }
    
    @Test
    void consume_shouldHandleInvalidJson() throws Exception {
        // Given: Invalid JSON
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0, "key", "{invalid");
        
        // When
        archiver.consume(record, acknowledgment);
        
        // Then: Should skip and acknowledge (to move past bad message)
        verify(acknowledgment, times(1)).acknowledge();
        
        RawEventArchiver.ArchiverStats stats = archiver.getStats();
        assertEquals(0, stats.currentBufferSize());
        assertEquals(0, stats.totalEventsProcessed());
    }
    
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = "Hadoop winutils.exe not available on Windows - run in Docker/WSL")
    @Test
    void shutdown_shouldFlushRemainingEvents() throws Exception {
        // Given: Events in buffer
        for (int i = 0; i < 5; i++) {
            ClickEvent event = createTestEvent(i);
            String json = objectMapper.writeValueAsString(event);
            ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, i, "key", json);
            archiver.consume(record, acknowledgment);
        }
        
        // When: Shutdown is called
        archiver.shutdown();
        
        // Then: Should have flushed buffered events
        RawEventArchiver.ArchiverStats stats = archiver.getStats();
        assertEquals(5, stats.totalEventsProcessed());
        assertEquals(1, stats.totalBatchesFlushed());
    }
    
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = "Hadoop winutils.exe not available on Windows - run in Docker/WSL")
    @Test
    void getStats_shouldReturnCurrentStatistics() throws Exception {
        // Given: Some events processed
        for (int i = 0; i < 15; i++) {
            ClickEvent event = createTestEvent(i);
            String json = objectMapper.writeValueAsString(event);
            ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, i, "key", json);
            archiver.consume(record, acknowledgment);
        }
        
        // When
        RawEventArchiver.ArchiverStats stats = archiver.getStats();
        
        // Then
        assertNotNull(stats);
        assertEquals(10, stats.totalEventsProcessed());  // First batch of 10
        assertEquals(1, stats.totalBatchesFlushed());
        assertEquals(5, stats.currentBufferSize());  // Remaining 5 in buffer
        assertNotNull(stats.lastFlush());
    }
    
    // Helper method
    
    private ClickEvent createTestEvent(int id) {
        EventMetadata metadata = EventMetadata.builder()
                .x(100)
                .y(200)
                .viewportWidth(1920)
                .viewportHeight(1080)
                .build();
        
        return ClickEvent.builder()
                .eventId("event-" + id)
                .userId("user-" + id)
                .sessionId("session-" + (id % 3))
                .eventType(EventType.CLICK)
                .targetElement("#button-" + id)
                .pageUrl("https://example.com/page")
                .referrerUrl("https://example.com/ref")
                .timestamp(System.currentTimeMillis())
                .userAgent("Test Agent")
                .metadata(metadata)
                .build();
    }
}
