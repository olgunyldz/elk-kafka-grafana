package io.github.olgunyldz.producer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Topic names, bound from {@code app.kafka.topics.*}.
 * The topics themselves are created by scripts/create-topics.sh.
 */
@ConfigurationProperties("app.kafka.topics")
public record TopicProperties(String logs, String events, String metrics) {
}
