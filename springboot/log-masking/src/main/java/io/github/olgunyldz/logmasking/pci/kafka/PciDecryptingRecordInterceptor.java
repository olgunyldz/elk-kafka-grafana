package io.github.olgunyldz.logmasking.pci.kafka;

import io.github.olgunyldz.logmasking.logging.PciLogger;
import io.github.olgunyldz.logmasking.pci.SensitiveDataEncryption;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.springframework.kafka.listener.RecordInterceptor;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.node.StringNode;

import java.util.List;
import java.util.Set;

/**
 * Decrypts values encrypted by {@link PciKafkaTemplate} before the listener sees the record,
 * but only for the consumer groups listed in {@code pci.decrypt.consumer-groups} ({@code *} = every
 * group). Every other group, and every other Kafka client, keeps seeing the tokens. Removed data
 * (CVV, PIN, passwords) can't come back: it never reached Kafka.
 */
public class PciDecryptingRecordInterceptor implements RecordInterceptor<Object, Object> {

    private static final Logger log = PciLogger.getLogger(PciDecryptingRecordInterceptor.class);

    private final SensitiveDataEncryption encryption;
    private final JsonMapper jsonMapper;
    private final Set<String> consumerGroups;

    public PciDecryptingRecordInterceptor(SensitiveDataEncryption encryption, JsonMapper jsonMapper, List<String> consumerGroups) {
        this.encryption = encryption;
        this.jsonMapper = jsonMapper;
        this.consumerGroups = Set.copyOf(consumerGroups);
    }

    @Override
    public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        Object value = record.value();
        if (value == null || record.headers().lastHeader(PciKafkaTemplate.PROTECTED_HEADER) == null) {
            return record;
        }
        if (!mayDecrypt(consumer)) {
            return record;
        }

        try {
            JsonNode decrypted = decrypt(jsonMapper.valueToTree(value));
            Object decryptedValue = jsonMapper.treeToValue(decrypted, value.getClass());
            return new ConsumerRecord<>(record.topic(), record.partition(), record.offset(),
                    record.timestamp(), record.timestampType(),
                    record.serializedKeySize(), record.serializedValueSize(),
                    record.key(), decryptedValue, record.headers(), record.leaderEpoch());
        } catch (IllegalArgumentException e) {
            // Wrong key or modified token: the listener gets the record as it is in Kafka, never a guess
            log.warn("Can't decrypt {} partition {} offset {}: producer and consumer need the same pci.data-encryption-key",
                    record.topic(), record.partition(), record.offset());
            return record;
        }
    }

    /** Whether this consumer's group may see decrypted data. */
    boolean mayDecrypt(Consumer<?, ?> consumer) {
        if (consumerGroups.contains("*")) return true;
        String group = consumer.groupMetadata().groupId();
        return group != null && consumerGroups.contains(group);
    }

    private JsonNode decrypt(JsonNode node) {
        if (node.isString()) {
            return StringNode.valueOf(encryption.decryptTokens(node.asString()));
        }
        if (node instanceof ObjectNode object) {
            for (String name : List.copyOf(object.propertyNames())) {
                object.set(name, decrypt(object.get(name)));
            }
        } else if (node instanceof ArrayNode array) {
            for (int i = 0; i < array.size(); i++) {
                array.set(i, decrypt(array.get(i)));
            }
        }
        return node;
    }
}
