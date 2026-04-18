package com.clickstream.etl.schema;

import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EventSchema.
 */
class EventSchemaTest {
    
    @Test
    @DisplayName("Should create valid ClickEvent schema")
    void testClickEventSchema() {
        StructType schema = EventSchema.clickEventSchema();
        
        assertNotNull(schema);
        assertNotNull(schema.getFieldIndex("eventId"));
        assertNotNull(schema.getFieldIndex("userId"));
        assertNotNull(schema.getFieldIndex("sessionId"));
        assertNotNull(schema.getFieldIndex("eventType"));
        assertNotNull(schema.getFieldIndex("pageUrl"));
        assertNotNull(schema.getFieldIndex("timestamp"));
        assertNotNull(schema.getFieldIndex("metadata"));
    }
    
    @Test
    @DisplayName("Should create valid EventMetadata schema")
    void testEventMetadataSchema() {
        StructType schema = EventSchema.metadataSchema();
        
        assertNotNull(schema);
        assertNotNull(schema.getFieldIndex("xCoordinate"));
        assertNotNull(schema.getFieldIndex("yCoordinate"));
        assertNotNull(schema.getFieldIndex("scrollDepth"));
    }
    
    @Test
    @DisplayName("Metadata schema should be nested in ClickEvent schema")
    void testNestedMetadataSchema() {
        StructType clickEventSchema = EventSchema.clickEventSchema();
        StructType metadataField = (StructType) clickEventSchema
                .fields()[clickEventSchema.getFieldIndex("metadata").get()]
                .dataType();
        
        assertNotNull(metadataField);
        assertNotNull(metadataField.getFieldIndex("scrollDepth"));
    }
}
