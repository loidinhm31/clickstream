package com.clickstream.archiver.writer;

import com.clickstream.archiver.config.ArchiverConfig;
import com.clickstream.model.ClickEvent;
import com.clickstream.model.EventMetadata;
import com.clickstream.model.EventType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParquetEventWriterTest {
    
    private ParquetEventWriter writer;
    private ArchiverConfig config;
    
    @TempDir
    java.nio.file.Path tempDir;
    
    @BeforeEach
    void setUp() {
        config = new ArchiverConfig();
        config.setDataLakeBasePath(tempDir.toString());
        config.getParquet().setCompression("SNAPPY");
        config.getParquet().setPageSize(1048576);
        config.getParquet().setRowGroupSize(134217728);
        
        writer = new ParquetEventWriter(config);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        // Cleanup is handled by @TempDir
    }
    
    @Test
    void write_shouldCreateParquetFile() throws IOException {
        // Given
        List<ClickEvent> events = createTestEvents(10);
        String filePath = tempDir.resolve("test.snappy.parquet").toString();
        
        // When
        writer.write(events, filePath);
        
        // Then
        assertTrue(Files.exists(java.nio.file.Path.of(filePath)));
    }
    
    @Test
    void write_shouldCreateParentDirectories() throws IOException {
        // Given
        List<ClickEvent> events = createTestEvents(5);
        String filePath = tempDir.resolve("nested/dir/structure/test.snappy.parquet").toString();
        
        // When
        writer.write(events, filePath);
        
        // Then
        assertTrue(Files.exists(java.nio.file.Path.of(filePath)));
    }
    
    @Test
    void write_shouldWriteCorrectNumberOfRecords() throws IOException {
        // Given
        List<ClickEvent> events = createTestEvents(100);
        String filePath = tempDir.resolve("test.snappy.parquet").toString();
        
        // When
        writer.write(events, filePath);
        
        // Then: Read back and verify count
        int recordCount = countRecords(filePath);
        assertEquals(100, recordCount);
    }
    
    @Test
    void write_shouldPreserveEventData() throws IOException {
        // Given
        ClickEvent originalEvent = createTestEvents(1).get(0);
        String filePath = tempDir.resolve("test.snappy.parquet").toString();
        
        // When
        writer.write(List.of(originalEvent), filePath);
        
        // Then: Read back and verify fields
        GenericRecord record = readFirstRecord(filePath);
        assertNotNull(record);
        assertEquals(originalEvent.getMetadata().getEventId(), record.get("eventId").toString());
        assertEquals(originalEvent.getUserId(), record.get("userId").toString());
        assertEquals(originalEvent.getSessionId(), record.get("sessionId").toString());
        assertEquals(originalEvent.getMetadata().getEventType().name(), record.get("eventType").toString());
        assertEquals(originalEvent.getPageUrl(), record.get("pageUrl").toString());
    }
    
    @Test
    void write_shouldHandleEmptyList() throws IOException {
        // Given
        List<ClickEvent> events = new ArrayList<>();
        String filePath = tempDir.resolve("test.snappy.parquet").toString();
        
        // When
        writer.write(events, filePath);
        
        // Then: File should not be created
        assertFalse(Files.exists(java.nio.file.Path.of(filePath)));
    }
    
    @Test
    void write_shouldHandleNullList() throws IOException {
        // Given
        String filePath = tempDir.resolve("test.snappy.parquet").toString();
        
        // When
        writer.write(null, filePath);
        
        // Then: Should not throw, file should not be created
        assertFalse(Files.exists(java.nio.file.Path.of(filePath)));
    }
    
    @Test
    void getSchema_shouldReturnValidAvroSchema() {
        // When
        Schema schema = writer.getSchema();
        
        // Then
        assertNotNull(schema);
        assertEquals("ClickEvent", schema.getName());
        assertEquals("com.clickstream.model", schema.getNamespace());
        assertTrue(schema.getFields().size() > 0);
        
        // Verify required fields exist
        assertNotNull(schema.getField("eventId"));
        assertNotNull(schema.getField("userId"));
        assertNotNull(schema.getField("sessionId"));
        assertNotNull(schema.getField("eventType"));
        assertNotNull(schema.getField("pageUrl"));
        assertNotNull(schema.getField("timestamp"));
    }
    
    // Helper methods
    
    private List<ClickEvent> createTestEvents(int count) {
        List<ClickEvent> events = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            EventMetadata metadata = EventMetadata.builder()
                    .eventId("event-" + i)
                    .eventType(EventType.CLICK)
                    .timestamp(Instant.now())
                    .schemaVersion("1.0")
                    .build();
            
            ClickEvent event = ClickEvent.builder()
                    .metadata(metadata)
                    .userId("user-" + i)
                    .sessionId("session-" + (i % 10))
                    .targetElement("#button-" + i)
                    .pageUrl("https://example.com/page-" + i)
                    .referrerUrl("https://example.com/ref")
                    .userAgent("Mozilla/5.0 Test")
                    .build();
            
            events.add(event);
        }
        return events;
    }
    
    private int countRecords(String filePath) throws IOException {
        int count = 0;
        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(new Path(filePath))
                .build()) {
            while (reader.read() != null) {
                count++;
            }
        }
        return count;
    }
    
    private GenericRecord readFirstRecord(String filePath) throws IOException {
        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(new Path(filePath))
                .build()) {
            return reader.read();
        }
    }
}
