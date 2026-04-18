package com.clickstream.archiver.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.*;

class PartitionPathBuilderTest {
    
    private PartitionPathBuilder pathBuilder;
    
    @BeforeEach
    void setUp() {
        pathBuilder = new PartitionPathBuilder("./data-lake");
    }
    
    @Test
    void buildPath_shouldCreateCorrectPartitionPath() {
        // Given: A timestamp from 2026-04-18 at 14:30:00 UTC
        Instant timestamp = ZonedDateTime.of(2026, 4, 18, 14, 30, 0, 0, ZoneOffset.UTC)
                .toInstant();
        
        // When
        String path = pathBuilder.buildPath(timestamp);
        
        // Then
        assertTrue(path.startsWith("./data-lake/raw-events/year=2026/month=04/day=18/hour=14/"),
                "Path should have correct partition structure");
        assertTrue(path.endsWith(".snappy.parquet"),
                "Path should end with .snappy.parquet");
        assertTrue(path.contains("part-00001-"),
                "Path should contain part number");
    }
    
    @Test
    void buildPath_shouldIncrementPartCounter() {
        // Given
        Instant timestamp = Instant.now();
        
        // When: Generate multiple paths
        String path1 = pathBuilder.buildPath(timestamp);
        String path2 = pathBuilder.buildPath(timestamp);
        String path3 = pathBuilder.buildPath(timestamp);
        
        // Then: Part numbers should increment
        assertTrue(path1.contains("part-00001-"));
        assertTrue(path2.contains("part-00002-"));
        assertTrue(path3.contains("part-00003-"));
    }
    
    @Test
    void buildPath_shouldHandleDifferentMonthsAndDays() {
        // Given: Different timestamps
        Instant jan1 = ZonedDateTime.of(2026, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant();
        Instant dec31 = ZonedDateTime.of(2026, 12, 31, 23, 0, 0, 0, ZoneOffset.UTC).toInstant();
        
        // When
        String janPath = pathBuilder.buildPath(jan1);
        String decPath = pathBuilder.buildPath(dec31);
        
        // Then
        assertTrue(janPath.contains("/year=2026/month=01/day=01/hour=00/"));
        assertTrue(decPath.contains("/year=2026/month=12/day=31/hour=23/"));
    }
    
    @Test
    void buildDirectoryPath_shouldReturnOnlyDirectory() {
        // Given
        Instant timestamp = ZonedDateTime.of(2026, 4, 18, 14, 30, 0, 0, ZoneOffset.UTC).toInstant();
        
        // When
        String dirPath = pathBuilder.buildDirectoryPath(timestamp);
        
        // Then
        assertEquals("./data-lake/raw-events/year=2026/month=04/day=18/hour=14", dirPath);
        assertFalse(dirPath.contains(".parquet"));
    }
    
    @Test
    void buildPath_shouldRemoveTrailingSlashFromBasePath() {
        // Given: Base path with trailing slash
        PartitionPathBuilder builder = new PartitionPathBuilder("./data-lake/");
        Instant timestamp = Instant.now();
        
        // When
        String path = builder.buildPath(timestamp);
        
        // Then: Should not have double slashes
        assertFalse(path.contains("//"));
    }
    
    @Test
    void resetCounter_shouldResetPartNumbering() {
        // Given
        Instant timestamp = Instant.now();
        pathBuilder.buildPath(timestamp);  // part-00001
        pathBuilder.buildPath(timestamp);  // part-00002
        
        // When
        pathBuilder.resetCounter();
        String path = pathBuilder.buildPath(timestamp);
        
        // Then
        assertTrue(path.contains("part-00001-"));
    }
}
