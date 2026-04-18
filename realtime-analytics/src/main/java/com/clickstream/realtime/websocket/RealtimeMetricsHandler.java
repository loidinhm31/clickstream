package com.clickstream.realtime.websocket;

import com.clickstream.realtime.engine.MetricsEngine;
import com.clickstream.realtime.engine.ArrowMetricsSnapshot;
import com.clickstream.realtime.serialization.ArrowIPCSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.BinaryWebSocketHandler;

import java.io.IOException;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WebSocket handler for real-time metrics push.
 * 
 * <p>Endpoint: /ws/realtime/metrics
 * <p>Protocol: Binary WebSocket (Arrow IPC frames)
 * <p>Push Interval: Configurable via metrics.websocket.push-interval-ms (default 1500ms)
 * <p>Rate Limiting: Max 5 connections per client IP
 * 
 * <p>Lifecycle:
 * 1. Client connects to /ws/realtime/metrics
 * 2. Connection validated (rate limit check)
 * 3. Session is registered in active sessions set
 * 4. Scheduler pushes metrics every 1.5s to all active sessions
 * 5. Client disconnects → session is removed, connection count decremented
 * 
 * <p>Error Handling:
 * - Broken connections are automatically detected and removed
 * - Serialization errors are logged but don't crash the handler
 * - Failed sends to individual clients don't affect other clients
 * - Rate-limited connections are immediately closed
 */
@Component
public class RealtimeMetricsHandler extends BinaryWebSocketHandler {

    private static final Logger logger = LoggerFactory.getLogger(RealtimeMetricsHandler.class);
    private static final int MAX_CONNECTIONS_PER_IP = 5;

    @Autowired
    private MetricsEngine metricsEngine;

    @Autowired
    private ArrowIPCSerializer serializer;

    @Value("${metrics.websocket.push-interval-ms:1500}")
    private int pushIntervalMs;

    // Thread-safe set of active WebSocket sessions
    private final Set<WebSocketSession> activeSessions = ConcurrentHashMap.newKeySet();
    
    // Rate limiting: track connection count per client IP
    private final Map<String, Integer> connectionCounts = new ConcurrentHashMap<>();

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String clientIP = extractClientIP(session);
        
        // Rate limit: check connection count for this IP
        int count = connectionCounts.merge(clientIP, 1, Integer::sum);
        if (count > MAX_CONNECTIONS_PER_IP) {
            logger.warn("Rate limit exceeded for IP: {}, rejecting connection", clientIP);
            try {
                session.close(CloseStatus.POLICY_VIOLATION.withReason("Too many connections"));
            } catch (IOException e) {
                logger.error("Failed to close rate-limited session", e);
            }
            connectionCounts.computeIfPresent(clientIP, (k, v) -> v > 1 ? v - 1 : null);
            return;
        }
        
        activeSessions.add(session);
        logger.info("WebSocket connected: sessionId={}, clientIP={}, total sessions={}",
                session.getId(), clientIP, activeSessions.size());

        // Send initial metrics snapshot immediately
        try {
            sendMetricsToSession(session);
        } catch (Exception e) {
            logger.error("Failed to send initial metrics to session: {}", session.getId(), e);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        activeSessions.remove(session);
        
        String clientIP = extractClientIP(session);
        connectionCounts.computeIfPresent(clientIP, (k, v) -> v > 1 ? v - 1 : null);
        
        logger.info("WebSocket disconnected: sessionId={}, clientIP={}, status={}, remaining sessions={}",
                session.getId(), clientIP, status, activeSessions.size());
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        logger.error("WebSocket transport error: sessionId={}", session.getId(), exception);
        activeSessions.remove(session);
    }

    /**
     * Scheduled task to push metrics to all connected clients.
     * Runs every 1.5 seconds (configurable via application.yml).
     */
    @Scheduled(fixedDelayString = "${metrics.websocket.push-interval-ms:1500}")
    public void pushMetrics() {
        if (activeSessions.isEmpty()) {
            return; // No clients connected, skip computation
        }

        try {
            // Compute metrics once
            ArrowMetricsSnapshot snapshot = metricsEngine.computeMetrics();
            byte[] arrowIPC = serializer.serializeToArrowIPC(snapshot);

            logger.debug("Broadcasting metrics to {} clients: {} bytes",
                    activeSessions.size(), arrowIPC.length);

            // Broadcast to all sessions
            BinaryMessage message = new BinaryMessage(arrowIPC);
            activeSessions.forEach(session -> {
                if (session.isOpen()) {
                    try {
                        session.sendMessage(message);
                    } catch (IOException e) {
                        logger.error("Failed to send metrics to session: {}", session.getId(), e);
                        activeSessions.remove(session);
                    }
                } else {
                    activeSessions.remove(session);
                }
            });
        } catch (Exception e) {
            logger.error("Failed to compute/broadcast metrics", e);
        }
    }

    /**
     * Send metrics to a specific session (used for initial connection).
     */
    private void sendMetricsToSession(WebSocketSession session) throws IOException {
        ArrowMetricsSnapshot snapshot = metricsEngine.computeMetrics();
        byte[] arrowIPC = serializer.serializeToArrowIPC(snapshot);
        session.sendMessage(new BinaryMessage(arrowIPC));
        logger.debug("Sent initial metrics to session: {}, {} bytes",
                session.getId(), arrowIPC.length);
    }

    /**
     * Extract client IP address from WebSocket session.
     * Handles X-Forwarded-For header for proxy scenarios.
     * 
     * @param session WebSocket session
     * @return Client IP address
     */
    private String extractClientIP(WebSocketSession session) {
        // Check X-Forwarded-For header first (for proxied connections)
        String forwardedFor = session.getHandshakeHeaders().getFirst("X-Forwarded-For");
        if (forwardedFor != null && !forwardedFor.isEmpty()) {
            return forwardedFor.split(",")[0].trim();
        }
        
        // Fall back to remote address
        if (session.getRemoteAddress() != null) {
            return session.getRemoteAddress().getAddress().getHostAddress();
        }
        
        return "unknown";
    }

    /**
     * Get count of active WebSocket connections.
     */
    public int getActiveSessionCount() {
        return activeSessions.size();
    }
}
