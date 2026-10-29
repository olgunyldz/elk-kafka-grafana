package io.github.olgunyldz.consumer.model;

import java.time.Instant;
import java.util.Map;

/** The consumer's own copy of the event contract, matched by the "event" type id. */
public record AppEvent(
        String id,
        Instant timestamp,
        String type,
        String aggregateId,
        Map<String, Object> payload) {
}
