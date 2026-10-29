package io.github.olgunyldz.producer.model;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * A measurement published to {@code app-metrics}. The Kafka key is {@link #service()},
 * so each service's measurements stay in time order.
 */
public record MetricMessage(
        String id,
        Instant timestamp,
        String service,
        String name,
        double value,
        Map<String, String> tags) {

    public static MetricMessage of(String service, String name, double value, Map<String, String> tags) {
        return new MetricMessage(UUID.randomUUID().toString(), Instant.now(), service, name, value, tags);
    }
}
