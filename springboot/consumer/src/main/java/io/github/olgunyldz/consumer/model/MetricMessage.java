package io.github.olgunyldz.consumer.model;

import java.time.Instant;
import java.util.Map;

/** The consumer's own copy of the metric contract, matched by the "metric" type id. */
public record MetricMessage(
        String id,
        Instant timestamp,
        String service,
        String name,
        double value,
        Map<String, String> tags) {
}
