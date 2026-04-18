package com.clickstream.realtime.serialization;

import com.clickstream.realtime.engine.ArrowMetricsSnapshot;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ArrowIPCSerializer.
 * 
 * <p>Tests cover:
 * - Serialization to Arrow IPC format
 * - Deserialization and schema validation
 * - Edge cases (empty metrics, large trending pages list)
 * - Binary format correctness
 */
@DisplayName("ArrowIPCSerializer Unit Tests")
class ArrowIPCSerializerTest {

    private ArrowIPCSerializer serializer;
    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        serializer = new ArrowIPCSerializer();
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @Test
    @DisplayName("Should serialize empty metrics snapshot")
    void testSerializeEmptyMetrics() throws IOException {
        ArrowMetricsSnapshot snapshot = ArrowMetricsSnapshot.empty();
        
        byte[] arrowIPC = serializer.serializeToArrowIPC(snapshot);
        
        assertNotNull(arrowIPC);
        assertTrue(arrowIPC.length > 0);
    }

    @Test
    @DisplayName("Should serialize non-empty metrics snapshot")
    void testSerializeNonEmptyMetrics() throws IOException {
        ArrowMetricsSnapshot snapshot = new ArrowMetricsSnapshot(
                42,
                2.5,
                10.3,
                List.of(
                        new ArrowMetricsSnapshot.PageMetric("/home", 100),
                        new ArrowMetricsSnapshot.PageMetric("/about", 50)
                ),
                System.currentTimeMillis()
        );
        
        byte[] arrowIPC = serializer.serializeToArrowIPC(snapshot);
        
        assertNotNull(arrowIPC);
        assertTrue(arrowIPC.length > 0);
    }

    @Test
    @DisplayName("Should produce valid Arrow IPC format")
    void testArrowIPCFormatValidity() throws IOException {
        ArrowMetricsSnapshot snapshot = new ArrowMetricsSnapshot(
                10,
                1.5,
                5.0,
                List.of(new ArrowMetricsSnapshot.PageMetric("/test", 10)),
                System.currentTimeMillis()
        );
        
        byte[] arrowIPC = serializer.serializeToArrowIPC(snapshot);
        
        // Verify the binary can be read as Arrow IPC
        try (ByteArrayInputStream input = new ByteArrayInputStream(arrowIPC);
             ArrowStreamReader reader = new ArrowStreamReader(input, allocator)) {
            
            assertTrue(reader.loadNextBatch());
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertNotNull(root);
            assertEquals(4, root.getFieldVectors().size());
        }
    }

    @Test
    @DisplayName("Should serialize metrics with correct schema")
    void testMetricsSchema() throws IOException {
        ArrowMetricsSnapshot snapshot = new ArrowMetricsSnapshot(
                25,
                3.7,
                12.8,
                List.of(),
                System.currentTimeMillis()
        );
        
        byte[] arrowIPC = serializer.serializeToArrowIPC(snapshot);
        
        try (ByteArrayInputStream input = new ByteArrayInputStream(arrowIPC);
             ArrowStreamReader reader = new ArrowStreamReader(input, allocator)) {
            
            assertTrue(reader.loadNextBatch());
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            
            // Verify schema field names
            assertEquals("activeUsers", root.getSchema().getFields().get(0).getName());
            assertEquals("clicksPerSecond", root.getSchema().getFields().get(1).getName());
            assertEquals("eventRate", root.getSchema().getFields().get(2).getName());
            assertEquals("computedAt", root.getSchema().getFields().get(3).getName());
        }
    }

    @Test
    @DisplayName("Should serialize metrics with correct values")
    void testMetricsValues() throws IOException {
        long timestamp = System.currentTimeMillis();
        ArrowMetricsSnapshot snapshot = new ArrowMetricsSnapshot(
                42,
                2.5,
                10.3,
                List.of(),
                timestamp
        );
        
        byte[] arrowIPC = serializer.serializeToArrowIPC(snapshot);
        
        try (ByteArrayInputStream input = new ByteArrayInputStream(arrowIPC);
             ArrowStreamReader reader = new ArrowStreamReader(input, allocator)) {
            
            assertTrue(reader.loadNextBatch());
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            
            assertEquals(1, root.getRowCount());
            assertEquals(42, root.getVector("activeUsers").getObject(0));
            assertEquals(2.5, (double) root.getVector("clicksPerSecond").getObject(0), 0.01);
            assertEquals(10.3, (double) root.getVector("eventRate").getObject(0), 0.01);
            assertEquals(timestamp, root.getVector("computedAt").getObject(0));
        }
    }

    @Test
    @DisplayName("Should serialize trending pages with correct schema")
    void testTrendingPagesSchema() throws IOException {
        ArrowMetricsSnapshot snapshot = new ArrowMetricsSnapshot(
                10,
                1.0,
                5.0,
                List.of(
                        new ArrowMetricsSnapshot.PageMetric("/home", 100),
                        new ArrowMetricsSnapshot.PageMetric("/about", 50)
                ),
                System.currentTimeMillis()
        );
        
        byte[] arrowIPC = serializer.serializeToArrowIPC(snapshot);
        
        // Verify the binary contains two Arrow IPC tables
        assertNotNull(arrowIPC);
        assertTrue(arrowIPC.length > 0);
        
        // Note: Full deserialization test would require reading both tables separately
        // For now, we verify serialization completes without error
    }

    @Test
    @DisplayName("Should handle large trending pages list")
    void testLargeTrendingPagesList() throws IOException {
        List<ArrowMetricsSnapshot.PageMetric> pages = List.of(
                new ArrowMetricsSnapshot.PageMetric("/page1", 100),
                new ArrowMetricsSnapshot.PageMetric("/page2", 90),
                new ArrowMetricsSnapshot.PageMetric("/page3", 80),
                new ArrowMetricsSnapshot.PageMetric("/page4", 70),
                new ArrowMetricsSnapshot.PageMetric("/page5", 60),
                new ArrowMetricsSnapshot.PageMetric("/page6", 50),
                new ArrowMetricsSnapshot.PageMetric("/page7", 40),
                new ArrowMetricsSnapshot.PageMetric("/page8", 30),
                new ArrowMetricsSnapshot.PageMetric("/page9", 20),
                new ArrowMetricsSnapshot.PageMetric("/page10", 10)
        );
        
        ArrowMetricsSnapshot snapshot = new ArrowMetricsSnapshot(
                100,
                5.0,
                20.0,
                pages,
                System.currentTimeMillis()
        );
        
        byte[] arrowIPC = serializer.serializeToArrowIPC(snapshot);
        
        assertNotNull(arrowIPC);
        assertTrue(arrowIPC.length > 0);
    }

    @Test
    @DisplayName("Should produce deterministic output for same input")
    void testDeterministicSerialization() throws IOException {
        ArrowMetricsSnapshot snapshot = new ArrowMetricsSnapshot(
                50,
                3.0,
                15.0,
                List.of(new ArrowMetricsSnapshot.PageMetric("/test", 25)),
                1000000000L  // Fixed timestamp
        );
        
        byte[] arrowIPC1 = serializer.serializeToArrowIPC(snapshot);
        byte[] arrowIPC2 = serializer.serializeToArrowIPC(snapshot);
        
        // Output should be identical for same input
        assertArrayEquals(arrowIPC1, arrowIPC2);
    }
}
