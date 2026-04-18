package com.clickstream.realtime.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * CORS Configuration for HTTP endpoints.
 * 
 * <p>Applies allowed origins from application.yml to all HTTP endpoints.
 * Ensures consistency between WebSocket and HTTP CORS policies.
 */
@Configuration
public class CorsConfig implements WebMvcConfigurer {

    @Value("${metrics.websocket.allowed-origins}")
    private String[] allowedOrigins;

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOrigins(allowedOrigins)
                .allowedMethods("GET", "POST", "PUT", "DELETE")
                .allowCredentials(true);
    }
}
