package io.github.olgunyldz.producer.service;

import io.github.olgunyldz.logmasking.logging.PciLogger;
import io.github.olgunyldz.logmasking.pci.kafka.PciKafkaTemplate;
import io.github.olgunyldz.logmasking.pci.kafka.PciKafkaTemplate.PciSendResult;
import io.github.olgunyldz.producer.config.TopicProperties;
import io.github.olgunyldz.producer.model.AppEvent;
import io.github.olgunyldz.producer.model.LogMessage;
import io.github.olgunyldz.producer.model.MetricMessage;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Sends every message through {@link PciKafkaTemplate}: card data and personal data are
 * removed or encrypted before the record leaves this service.
 */
@Service
public class KafkaPublisher {

    // PciLogger masks card numbers, CVV, IBAN, national ID, email, phone and secrets in every line it writes
    private static final Logger log = PciLogger.getLogger(KafkaPublisher.class);

    private final PciKafkaTemplate kafkaTemplate;
    private final TopicProperties topics;

    public KafkaPublisher(PciKafkaTemplate kafkaTemplate, TopicProperties topics) {
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
    }

    public CompletableFuture<PciSendResult> publish(LogMessage message) {
        return send(topics.logs(), message.service(), message);
    }

    public CompletableFuture<PciSendResult> publish(AppEvent event) {
        return send(topics.events(), event.aggregateId(), event);
    }

    public CompletableFuture<PciSendResult> publish(MetricMessage metric) {
        return send(topics.metrics(), metric.service(), metric);
    }

    private CompletableFuture<PciSendResult> send(String topic, String key, Object value) {
        // As received: PciLogger masks the values, the raw data never reaches the log
        log.info("Received for {}: key={} value={}", topic, key, value);

        return kafkaTemplate.send(topic, key, value).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send to {}: key={}", topic, key, ex);
                return;
            }
            // As sent: what's really in Kafka (sensitive data removed or encrypted)
            var meta = result.sendResult().getRecordMetadata();
            log.info("Sent to {} partition {} offset {}: key={} value={} (removed {}, encrypted {})",
                    meta.topic(), meta.partition(), meta.offset(), key, result.sentValue(),
                    result.removedValues().size(), result.encryptedValues().size());
        });
    }
}
