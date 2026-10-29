package io.github.olgunyldz.producer.service;

import io.github.olgunyldz.producer.config.TopicProperties;
import io.github.olgunyldz.producer.model.AppEvent;
import io.github.olgunyldz.producer.model.LogMessage;
import io.github.olgunyldz.producer.model.MetricMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TopicProperties topics;

    public KafkaPublisher(KafkaTemplate<String, Object> kafkaTemplate, TopicProperties topics) {
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
    }

    public CompletableFuture<SendResult<String, Object>> publish(LogMessage message) {
        return send(topics.logs(), message.service(), message);
    }

    public CompletableFuture<SendResult<String, Object>> publish(AppEvent event) {
        return send(topics.events(), event.aggregateId(), event);
    }

    public CompletableFuture<SendResult<String, Object>> publish(MetricMessage metric) {
        return send(topics.metrics(), metric.service(), metric);
    }

    private CompletableFuture<SendResult<String, Object>> send(String topic, String key, Object value) {
        return kafkaTemplate.send(topic, key, value).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send to {} (key={})", topic, key, ex);
            } else {
                var meta = result.getRecordMetadata();
                log.debug("Sent to {}-{}@{} (key={})", meta.topic(), meta.partition(), meta.offset(), key);
            }
        });
    }
}
