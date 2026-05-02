package com.clickstream.archiver.config;

import com.clickstream.validation.EventValidator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers EventValidator as Spring bean since shared-models
 * is a plain JAR and doesn't have Spring annotations.
 */
@Configuration
public class SharedModelConfig {

    @Bean
    public EventValidator eventValidator() {
        return new EventValidator();
    }
}
