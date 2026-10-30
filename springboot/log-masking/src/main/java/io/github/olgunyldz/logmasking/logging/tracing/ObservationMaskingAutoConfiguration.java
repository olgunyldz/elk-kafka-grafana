package io.github.olgunyldz.logmasking.logging.tracing;

import io.micrometer.observation.ObservationFilter;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Registers {@link MaskingObservationFilter} in any Spring Boot app that uses Micrometer observations
 * (tracing, HTTP and Kafka metrics). Turned off with {@code logging.masking.enabled=false}, like the
 * log masking. Listed in META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports.
 */
@AutoConfiguration
@ConditionalOnClass(ObservationFilter.class)
@ConditionalOnBooleanProperty(name = "logging.masking.enabled", matchIfMissing = true)
public class ObservationMaskingAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    MaskingObservationFilter maskingObservationFilter() {
        return new MaskingObservationFilter();
    }
}
