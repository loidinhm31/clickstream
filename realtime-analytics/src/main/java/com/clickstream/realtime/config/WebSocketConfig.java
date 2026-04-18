package com.clickstream.realtime.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import com.clickstream.realtime.websocket.RealtimeMetricsHandler;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * WebSocket Configuration for Real-time Metrics Push.
 * 
 * <p>Endpoint: /ws/realtime/metrics
 * <p>Protocol: Binary WebSocket (no STOMP - direct binary Arrow IPC frames)
 * <p>Push Interval: 1.5 seconds (configured via @Scheduled in handler)
 * 
 * <p>CORS: Allows frontend origins from application.yml configuration.
 * Browsers require explicit origin validation for WebSocket connections.
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    @Value("${metrics.websocket.allowed-origins}")
    private String[] allowedOrigins;

    @Autowired
    private RealtimeMetricsHandler metricsHandler;

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(metricsHandler, "/ws/realtime/metrics")
                .setAllowedOrigins(allowedOrigins);
    }
}
