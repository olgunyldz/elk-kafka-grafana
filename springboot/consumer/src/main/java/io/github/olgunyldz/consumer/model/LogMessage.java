package io.github.olgunyldz.consumer.model;

import java.time.Instant;

/**
 * The consumer's own copy of the log contract. It is matched to the producer's
 * class by the "log" type id, not by class name, so the two services stay independent.
 */
public record LogMessage(
        String id,
        Instant timestamp,
        String service,
        String level,
        String message,
        String traceId) {
}
