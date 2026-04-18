package com.clickstream.realtime.serialization;

import com.clickstream.realtime.engine.ArrowMetricsSnapshot;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Serializer for converting ArrowMetricsSnapshot to Apache Arrow IPC format.
 * 
 * <p>Output Format: Arrow IPC Stream (application/octet-stream)
 * <p>Protocol: Arrow IPC binary format - can be consumed by:
 * - JavaScript: apache-arrow npm package (tableFromIPC())
 * - Python: pyarrow.ipc.open_stream()
 * - Java: ArrowStreamReader
 * 
 * <p>Schema:
 * <pre>
 * Root:
 *   - activeUsers: Int32
 *   - clicksPerSecond: Float64
 *   - eventRate: Float64
 *   - computedAt: Int64 (epoch millis)
 * TrendingPages (nested):
 *   - pageUrl: Utf8
 *   - viewCount: Int32
 * </pre>
 * 
 * <p>Memory Management:
 * Uses short-lived BufferAllocator for each serialization.
 * Allocator and VectorSchemaRoot are closed via try-with-resources.
 */
@Component
public class ArrowIPCSerializer {

    private static final Logger logger = LoggerFactory.getLogger(ArrowIPCSerializer.class);

    // Schema for metrics root table
    private static final Schema METRICS_SCHEMA = new Schema(Arrays.asList(
        new Field("activeUsers", FieldType.nullable(new ArrowType.Int(32, true)), null),
        new Field("clicksPerSecond", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
        new Field("eventRate", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
        new Field("computedAt", FieldType.nullable(new ArrowType.Int(64, true)), null)
    ));

    // Schema for trending pages table
    private static final Schema TRENDING_PAGES_SCHEMA = new Schema(Arrays.asList(
        new Field("pageUrl", FieldType.nullable(new ArrowType.Utf8()), null),
        new Field("viewCount", FieldType.nullable(new ArrowType.Int(32, true)), null)
    ));

    /**
     * Serialize metrics snapshot to Arrow IPC binary format.
     * 
     * <p>Output contains two Arrow tables in a single IPC stream:
     * 1. Metrics table (1 row with aggregate metrics)
     * 2. Trending pages table (up to 10 rows)
     * 
     * @param snapshot Metrics snapshot to serialize
     * @return Arrow IPC binary data
     * @throws IOException if serialization fails
     */
    public byte[] serializeToArrowIPC(ArrowMetricsSnapshot snapshot) throws IOException {
        try (BufferAllocator allocator = new RootAllocator()) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();

            // Serialize metrics table
            try (VectorSchemaRoot metricsRoot = buildMetricsRoot(allocator, snapshot);
                 ArrowStreamWriter writer = new ArrowStreamWriter(metricsRoot, null, output)) {
                writer.start();
                writer.writeBatch();
            }

            // Serialize trending pages table
            try (VectorSchemaRoot pagesRoot = buildTrendingPagesRoot(allocator, snapshot);
                 ArrowStreamWriter writer = new ArrowStreamWriter(pagesRoot, null, output)) {
                writer.start();
                writer.writeBatch();
            }

            byte[] result = output.toByteArray();
            logger.debug("Serialized metrics to Arrow IPC: {} bytes", result.length);
            return result;
        }
    }

    /**
     * Build Arrow table for aggregate metrics (single row).
     */
    private VectorSchemaRoot buildMetricsRoot(BufferAllocator allocator, ArrowMetricsSnapshot snapshot) {
        VectorSchemaRoot root = VectorSchemaRoot.create(METRICS_SCHEMA, allocator);
        root.setRowCount(1);

        IntVector activeUsersVector = (IntVector) root.getVector("activeUsers");
        Float8Vector clicksPerSecondVector = (Float8Vector) root.getVector("clicksPerSecond");
        Float8Vector eventRateVector = (Float8Vector) root.getVector("eventRate");
        BigIntVector computedAtVector = (BigIntVector) root.getVector("computedAt");

        activeUsersVector.setSafe(0, snapshot.activeUsers());
        clicksPerSecondVector.setSafe(0, snapshot.clicksPerSecond());
        eventRateVector.setSafe(0, snapshot.eventRate());
        computedAtVector.setSafe(0, snapshot.computedAt());

        return root;
    }

    /**
     * Build Arrow table for trending pages (up to 10 rows).
     */
    private VectorSchemaRoot buildTrendingPagesRoot(BufferAllocator allocator, ArrowMetricsSnapshot snapshot) {
        VectorSchemaRoot root = VectorSchemaRoot.create(TRENDING_PAGES_SCHEMA, allocator);
        root.setRowCount(snapshot.trendingPages().size());

        VarCharVector pageUrlVector = (VarCharVector) root.getVector("pageUrl");
        IntVector viewCountVector = (IntVector) root.getVector("viewCount");

        for (int i = 0; i < snapshot.trendingPages().size(); i++) {
            ArrowMetricsSnapshot.PageMetric page = snapshot.trendingPages().get(i);
            pageUrlVector.setSafe(i, page.pageUrl().getBytes());
            viewCountVector.setSafe(i, page.viewCount());
        }

        return root;
    }
}
