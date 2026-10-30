package io.github.olgunyldz.consumer.config;

import io.micrometer.observation.ObservationPredicate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.observation.ServerRequestObservationContext;

@Configuration
public class TracingConfig {

    /**
     * No traces (or HTTP metrics) for /actuator: Prometheus scrapes and health checks call it every few
     * seconds, and those traces would bury the real ones in Tempo.
     */
    @Bean
    ObservationPredicate noActuatorObservations() {
        return (name, context) -> !(context instanceof ServerRequestObservationContext request
                && request.getCarrier().getRequestURI().startsWith("/actuator"));
    }
}
