package com.clickstream.realtime.controller;

import com.clickstream.realtime.engine.MetricsEngine;
import com.clickstream.realtime.engine.ArrowMetricsSnapshot;
import com.clickstream.realtime.serialization.ArrowIPCSerializer;
import com.clickstream.realtime.websocket.RealtimeMetricsHandler;
import com.clickstream.realtime.kafka.EventConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST controller for real-time metrics.
 * 
 * <p>Endpoints:
 * - GET /api/realtime/metrics → Arrow IPC binary (pull-based fallback)
 * - GET /api/realtime/health → Service health check (including Kafka consumer status)
 * - GET /api/realtime/stats → Engine statistics (monitoring)
 * 
 * <p>Use Cases:
 * - HTTP endpoint: Fallback for clients that can't use WebSocket (polling)
 * - Health check: Kubernetes/Docker health probes
 * - Stats: Monitoring dashboards (Prometheus/Grafana)
 */
@RestController
@RequestMapping("/api/realtime")
public class RealtimeController {

    private static final Logger logger = LoggerFactory.getLogger(RealtimeController.class);

    @Autowired
    private MetricsEngine metricsEngine;

    @Autowired
    private ArrowIPCSerializer serializer;

    @Autowired
    private RealtimeMetricsHandler websocketHandler;

    @Autowired
    private EventConsumer eventConsumer;

    @Value("${metrics.websocket.allowed-origins}")
    private String[] allowedOrigins;

    /**
     * Get current metrics as Arrow IPC binary.
     * 
     * <p>Response: application/octet-stream (Arrow IPC format)
     * <p>Client: Use apache-arrow npm package: tableFromIPC(buffer)
     * 
     * @return Arrow IPC binary data
     */
    @CrossOrigin // Uses allowed-origins from config
    @GetMapping(value = "/metrics", produces = "application/octet-stream")
    public ResponseEntity<byte[]> getMetrics() {
        try {
            ArrowMetricsSnapshot snapshot = metricsEngine.computeMetrics();
            byte[] arrowIPC = serializer.serializeToArrowIPC(snapshot);

            logger.debug("HTTP metrics request: {} bytes", arrowIPC.length);

            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body(arrowIPC);
        } catch (Exception e) {
            logger.error("Failed to serialize metrics", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Includes Kafka consumer status and ring buffer health.
     * 
     * @return 200 OK if service is healthy, 503 if degraded
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        boolean kafkaHealthy = eventConsumer.isHealthy();
        Map<String, Object> stats = metricsEngine.getStats();
        long batchCount = (Long) stats.get("batchCount");
        
        // Service is degraded if Kafka consumer is down OR no events in last 5 min
        boolean isHealthy = kafkaHealthy && batchCount > 0;
        String status = isHealthy ? "UP" : (kafkaHealthy ? "DEGRADED" : "DOWN");
        
        return ResponseEntity.status(isHealthy ? 200 : 503).body(Map.of(
                "status", status,
                "service", "realtime-analytics",
                "kafka", kafkaHealthy ? "UP" : "DOWN",
                "ringBufferBatches", batchCount,
                "activeWebSocketSessions", websocketHandler.getActiveSessionCount()
        ));
    }

    /**
     * Get engine statistics for monitoring.
     * 
     * <p>Returns:
     * - Ring buffer batch count
     * - Arrow allocator memory usage
     * - Active WebSocket sessions
     * 
     * @return Statistics JSON
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> stats() {
        Map<String, Object> engineStats = metricsEngine.getStats();
        engineStats.put("activeWebSocketSessions", websocketHandler.getActiveSessionCount());
        return ResponseEntity.ok(engineStats);
    }
}
