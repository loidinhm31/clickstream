package com.clickstream.archiver.writer;

import com.clickstream.model.ClickEvent;
import com.clickstream.archiver.config.ArchiverConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Writes ClickEvent objects to Parquet files with Snappy compression.
 * 
 * Thread-safe for concurrent writes to different files.
 */
@Component
@Slf4j
public class ParquetEventWriter {
    
    private final ArchiverConfig config;
    private final Schema avroSchema;
    
    public ParquetEventWriter(ArchiverConfig config) {
        this.config = config;
        this.avroSchema = createAvroSchema();
    }
    
    /**
     * Writes a batch of events to a Parquet file at the specified path.
     * Creates parent directories if they don't exist.
     * 
     * @param events List of events to write
     * @param filePath Full path for the Parquet file
     * @throws IOException if write fails
     */
    public void write(List<ClickEvent> events, String filePath) throws IOException {
        if (events == null || events.isEmpty()) {
            log.warn("Attempted to write empty event list to {}", filePath);
            return;
        }
        
        // Ensure parent directory exists
        File file = new File(filePath);
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            if (parentDir.mkdirs()) {
                log.debug("Created directory: {}", parentDir.getAbsolutePath());
            }
        }
        
        Path path = new Path(filePath);
        CompressionCodecName compression = CompressionCodecName.valueOf(
            config.getParquet().getCompression().toUpperCase()
        );
        
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withSchema(avroSchema)
                .withCompressionCodec(compression)
                .withPageSize(config.getParquet().getPageSize())
                .withRowGroupSize(config.getParquet().getRowGroupSize())
                .build()) {
            
            for (ClickEvent event : events) {
                GenericRecord record = convertToAvroRecord(event);
                writer.write(record);
            }
            
            log.info("Successfully wrote {} events to {}", events.size(), filePath);
        } catch (IOException e) {
            log.error("Failed to write {} events to {}: {}", events.size(), filePath, e.getMessage());  // No stack trace (OWASP compliance)
            throw e;
        }
    }
    
    /**
     * Converts a ClickEvent to an Avro GenericRecord.
     */
    private GenericRecord convertToAvroRecord(ClickEvent event) {
        GenericRecord record = new GenericData.Record(avroSchema);
        
        record.put("eventId", event.getEventId());
        record.put("userId", event.getUserId());
        record.put("sessionId", event.getSessionId());
        record.put("eventType", event.getEventType().name());
        record.put("targetElement", event.getTargetElement());
        record.put("pageUrl", event.getPageUrl());
        record.put("referrerUrl", event.getReferrerUrl());
        record.put("timestamp", event.getTimestamp());
        record.put("userAgent", event.getUserAgent());
        
        // Schema version from ClickEvent
        record.put("schemaVersion", event.getSchemaVersion());
        
        return record;
    }
    
    /**
     * Creates the Avro schema for ClickEvent.
     * Mirrors the ClickEvent class structure.
     */
    private Schema createAvroSchema() {
        String schemaJson = """
            {
              "type": "record",
              "name": "ClickEvent",
              "namespace": "com.clickstream.model",
              "fields": [
                {"name": "eventId", "type": "string"},
                {"name": "userId", "type": "string"},
                {"name": "sessionId", "type": "string"},
                {"name": "eventType", "type": "string"},
                {"name": "targetElement", "type": ["null", "string"], "default": null},
                {"name": "pageUrl", "type": "string"},
                {"name": "referrerUrl", "type": ["null", "string"], "default": null},
                {"name": "timestamp", "type": "long"},
                {"name": "userAgent", "type": ["null", "string"], "default": null},
                {"name": "schemaVersion", "type": "string"}
              ]
            }
            """;
        
        return new Schema.Parser().parse(schemaJson);
    }
    
    /**
     * Returns the Avro schema used for writing.
     * Useful for testing and validation.
     */
    public Schema getSchema() {
        return avroSchema;
    }
}
