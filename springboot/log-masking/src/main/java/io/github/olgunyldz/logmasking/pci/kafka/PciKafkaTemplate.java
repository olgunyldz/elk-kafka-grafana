package io.github.olgunyldz.logmasking.pci.kafka;

import io.github.olgunyldz.logmasking.pci.SensitiveDataDetector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Sends to Kafka like {@link KafkaTemplate}, but first removes card security data and credentials,
 * and encrypts card and personal data in the value. What reaches the broker never contains them in
 * readable form.
 * <p>
 * It wraps the KafkaTemplate rather than extending it, so there is no inherited send method that
 * skips the protection. In a Spring Boot app, {@link PciKafkaAutoConfiguration} creates it: inject
 * {@code PciKafkaTemplate} instead of {@code KafkaTemplate}.
 */
public class PciKafkaTemplate {

    /** Set on records that contain encrypted values, so consumers can skip the rest cheaply. */
    public static final String PROTECTED_HEADER = "pci-protected";

    /**
     * @param sendResult      Kafka's result; {@code getProducerRecord().value()} is the value as it was sent
     * @param removedValues   parts of the message that were deleted, for example {@code payload.cvv (CVV)}
     * @param encryptedValues parts that were encrypted, for example {@code payload.cardNumber (card number)}
     */
    public record PciSendResult(SendResult<String, Object> sendResult,
                                List<String> removedValues,
                                List<String> encryptedValues) {

        /** The value exactly as Kafka stores it: removed parts gone, encrypted parts as tokens. */
        public Object sentValue() {
            return sendResult.getProducerRecord().value();
        }
    }

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final SensitiveDataDetector detector;
    private final JsonMapper jsonMapper;

    public PciKafkaTemplate(KafkaTemplate<String, Object> kafkaTemplate, SensitiveDataDetector detector, JsonMapper jsonMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.detector = detector;
        this.jsonMapper = jsonMapper;
    }

    /** Sends without a key: the partitioner picks the partition. */
    public CompletableFuture<PciSendResult> send(String topic, Object value) {
        return send(new ProducerRecord<>(topic, value));
    }

    /**
     * @throws IllegalArgumentException if the key contains card or personal data. Keys can't be
     *                                  encrypted: a random IV would send equal keys to different partitions.
     */
    public CompletableFuture<PciSendResult> send(String topic, String key, Object value) {
        return send(new ProducerRecord<>(topic, key, value));
    }

    /** Sends a full record; its partition, timestamp and headers are kept. */
    public CompletableFuture<PciSendResult> send(ProducerRecord<String, Object> record) {
        checkKey(record.key());

        Object value = record.value();
        var result = value == null ? null : detector.protect(jsonMapper.valueToTree(value));
        // Convert back to the original type, so the JSON serializer still writes the right type id
        Object protectedValue = result == null || result.findings().isEmpty()
                ? value
                : jsonMapper.treeToValue(result.node(), value.getClass());

        var protectedRecord = new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
                record.key(), protectedValue, record.headers());
        if (result != null && !result.encrypted().isEmpty()) {
            protectedRecord.headers().add(PROTECTED_HEADER, "v1".getBytes(StandardCharsets.UTF_8));
        }
        List<String> removed = result == null ? List.of() : result.removed();
        List<String> encrypted = result == null ? List.of() : result.encrypted();
        return kafkaTemplate.send(protectedRecord).thenApply(sent -> new PciSendResult(sent, removed, encrypted));
    }

    /** Blocks until every record sent so far has been sent. */
    public void flush() {
        kafkaTemplate.flush();
    }

    private void checkKey(String key) {
        if (key == null) return;
        var inKey = detector.findInText(key);
        if (!inKey.isEmpty()) {
            throw new IllegalArgumentException("Kafka key must not contain " + String.join(", ", inKey)
                    + "; use a non-sensitive id as the key");
        }
    }
}
