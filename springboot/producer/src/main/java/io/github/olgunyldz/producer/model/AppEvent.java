package io.github.olgunyldz.producer.model;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * A business event published to {@code app-events}. The Kafka key is {@link #aggregateId()},
 * so all events for one entity (for example one order) are consumed in order.
 */
public record AppEvent(
        String id,
        Instant timestamp,
        String type,
        String aggregateId,
        Map<String, Object> payload) {

    public static AppEvent of(String type, String aggregateId, Map<String, Object> payload) {
        return new AppEvent(UUID.randomUUID().toString(), Instant.now(), type, aggregateId, payload);
    }
}
