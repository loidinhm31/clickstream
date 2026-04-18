package com.clickstream.archiver.util;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Builds date-partitioned directory paths and filenames for Parquet files.
 * 
 * Directory structure: {basePath}/raw-events/year={yyyy}/month={MM}/day={dd}/hour={HH}/part-{nnnnn}-{timestamp}.snappy.parquet
 * 
 * Example: ./data-lake/raw-events/year=2026/month=04/day=18/hour=14/part-00001-1713451200.snappy.parquet
 * 
 * Thread-safe for concurrent path generation.
 */
public class PartitionPathBuilder {
    
    private final String basePath;
    private final AtomicLong partCounter = new AtomicLong(0);
    
    public PartitionPathBuilder(String basePath) {
        this.basePath = basePath.endsWith("/") ? basePath.substring(0, basePath.length() - 1) : basePath;
    }
    
    /**
     * Builds the full partition path for a given timestamp.
     * 
     * @param timestamp Event timestamp
     * @return Full path including filename (e.g., ./data-lake/raw-events/year=2026/.../part-00001-1713451200.snappy.parquet)
     */
    public String buildPath(Instant timestamp) {
        ZonedDateTime zdt = timestamp.atZone(ZoneOffset.UTC);
        
        String directoryPath = String.format(
            "%s/raw-events/year=%d/month=%02d/day=%02d/hour=%02d",
            basePath,
            zdt.getYear(),
            zdt.getMonthValue(),
            zdt.getDayOfMonth(),
            zdt.getHour()
        );
        
        String filename = String.format(
            "part-%05d-%d.snappy.parquet",
            partCounter.incrementAndGet(),
            timestamp.getEpochSecond()
        );
        
        return directoryPath + "/" + filename;
    }
    
    /**
     * Builds only the directory path (without filename) for a given timestamp.
     * 
     * @param timestamp Event timestamp
     * @return Directory path (e.g., ./data-lake/raw-events/year=2026/month=04/day=18/hour=14)
     */
    public String buildDirectoryPath(Instant timestamp) {
        ZonedDateTime zdt = timestamp.atZone(ZoneOffset.UTC);
        
        return String.format(
            "%s/raw-events/year=%d/month=%02d/day=%02d/hour=%02d",
            basePath,
            zdt.getYear(),
            zdt.getMonthValue(),
            zdt.getDayOfMonth(),
            zdt.getHour()
        );
    }
    
    /**
     * Resets the part counter (useful for testing).
     */
    public void resetCounter() {
        partCounter.set(0);
    }
}
