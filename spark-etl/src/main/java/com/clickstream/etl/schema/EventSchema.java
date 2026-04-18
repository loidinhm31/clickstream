package com.clickstream.etl.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Spark SQL schema definitions for clickstream events.
 * 
 * <p>Provides StructType schemas matching the JSON schema from Phase 2.
 * Used for parsing Kafka JSON messages into Spark DataFrames.
 */
public class EventSchema {

    /**
     * Schema for EventMetadata nested object.
     */
    public static StructType metadataSchema() {
        return new StructType()
                .add("version", DataTypes.StringType, true)
                .add("xCoordinate", DataTypes.IntegerType, true)
                .add("yCoordinate", DataTypes.IntegerType, true)
                .add("scrollDepth", DataTypes.DoubleType, true)
                .add("viewportWidth", DataTypes.IntegerType, true)
                .add("viewportHeight", DataTypes.IntegerType, true)
                .add("deviceType", DataTypes.StringType, true)
                .add("screenResolution", DataTypes.StringType, true);
    }

    /**
     * Schema for ClickEvent (top-level event schema).
     * 
     * <p>Matches the JSON structure from shared-models ClickEvent class.
     * 
     * @return StructType for ClickEvent
     */
    public static StructType clickEventSchema() {
        return new StructType()
                .add("schemaVersion", DataTypes.StringType, true)
                .add("eventId", DataTypes.StringType, false)
                .add("userId", DataTypes.StringType, false)
                .add("sessionId", DataTypes.StringType, false)
                .add("eventType", DataTypes.StringType, false)
                .add("targetElement", DataTypes.StringType, true)
                .add("pageUrl", DataTypes.StringType, false)
                .add("referrerUrl", DataTypes.StringType, true)
                .add("timestamp", DataTypes.LongType, false)
                .add("userAgent", DataTypes.StringType, true)
                .add("metadata", metadataSchema(), true);
    }

    /**
     * Private constructor to prevent instantiation.
     */
    private EventSchema() {
        throw new UnsupportedOperationException("Utility class");
    }
}
