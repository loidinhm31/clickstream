package com.clickstream.config;

import com.clickstream.validation.EventValidator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Bean configuration for shared-models components.
 * 
 * Registers EventValidator as Spring bean since shared-models
 * is a library module without Spring dependencies.
 */
@Configuration
public class SharedModelConfig {

    @Bean
    public EventValidator eventValidator() {
        return new EventValidator();
    }
}
