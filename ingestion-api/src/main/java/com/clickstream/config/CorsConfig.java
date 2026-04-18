package com.clickstream.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Global CORS configuration for frontend access.
 * 
 * Allows requests from configured origins (default: http://localhost:3000)
 * Required for browser-based frontend to call ingestion and analytics APIs
 */
@Configuration
public class CorsConfig implements WebMvcConfigurer {

    @Value("${clickstream.cors.allowed-origins:http://localhost:3000}")
    private String[] allowedOrigins;

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOrigins(allowedOrigins)
                .allowedMethods("GET", "POST", "OPTIONS")  // Only needed methods
                .allowedHeaders("Content-Type", "Accept")  // Specific headers only
                .allowCredentials(false)  // Disable credentials for security
                .maxAge(3600);  // Cache preflight response for 1 hour
    }
}
