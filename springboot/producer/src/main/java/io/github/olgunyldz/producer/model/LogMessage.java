package io.github.olgunyldz.producer.model;

import java.time.Instant;
import java.util.UUID;

/**
 * A log line published to {@code app-logs}. The Kafka key is {@link #service()},
 * so all logs from one service stay in order on the same partition.
 */
public record LogMessage(
        String id,
        Instant timestamp,
        String service,
        String level,
        String message,
        String traceId) {

    public static LogMessage of(String service, String level, String message) {
        return new LogMessage(
                UUID.randomUUID().toString(),
                Instant.now(),
                service,
                level,
                message,
                UUID.randomUUID().toString().substring(0, 16));
    }
}
