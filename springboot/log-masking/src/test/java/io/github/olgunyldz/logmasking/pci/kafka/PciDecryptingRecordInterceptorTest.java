package io.github.olgunyldz.logmasking.pci.kafka;

import io.github.olgunyldz.logmasking.pci.SensitiveDataEncryption;
import io.github.olgunyldz.logmasking.pci.SensitiveDataEncryption.Type;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PciDecryptingRecordInterceptorTest {

    record LogLine(String service, String message, Map<String, Object> details) {
    }

    private final SensitiveDataEncryption encryption = new SensitiveDataEncryption(
            "+WpfE866Ysy6Lp51Q61eBAcupVyUtI5nnyxwm6avujc=");
    private final PciDecryptingRecordInterceptor interceptor = new PciDecryptingRecordInterceptor(
            encryption, new JsonMapper(), List.of("log-consumer"));

    private ConsumerRecord<Object, Object> record(Object value, boolean protectedHeader) {
        var record = new ConsumerRecord<Object, Object>("app-logs", 0, 5, "payment-service", value);
        if (protectedHeader) {
            record.headers().add(PciKafkaTemplate.PROTECTED_HEADER, "v1".getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    @SuppressWarnings("unchecked")
    private Consumer<Object, Object> consumerInGroup(String groupId) {
        Consumer<Object, Object> consumer = mock(Consumer.class);
        when(consumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata(groupId));
        return consumer;
    }

    private LogLine encryptedLine(SensitiveDataEncryption with) {
        return new LogLine("payment-service",
                "Declined card " + with.encrypt(Type.PAN, "4111111111111111"),
                Map.of("email", with.encrypt(Type.EMAIL, "jane@example.com")));
    }

    @Test
    void decryptsForListedGroups() {
        var result = interceptor.intercept(record(encryptedLine(encryption), true), consumerInGroup("log-consumer"));

        var value = (LogLine) result.value();
        assertThat(value.message()).isEqualTo("Declined card 4111111111111111");
        assertThat(value.details()).containsEntry("email", "jane@example.com");
        assertThat(result.offset()).isEqualTo(5);
        assertThat(result.key()).isEqualTo("payment-service");
    }

    @Test
    void leavesRecordsEncryptedForOtherGroups() {
        var original = record(encryptedLine(encryption), true);

        assertThat(interceptor.intercept(original, consumerInGroup("analytics"))).isSameAs(original);
    }

    @Test
    void starAllowsEveryGroup() {
        var everyone = new PciDecryptingRecordInterceptor(encryption, new JsonMapper(), List.of("*"));

        var result = everyone.intercept(record(encryptedLine(encryption), true), consumerInGroup("analytics"));

        assertThat(((LogLine) result.value()).message()).isEqualTo("Declined card 4111111111111111");
    }

    @Test
    void passesTheRecordOnUnchangedWhenTheKeyIsWrong() {
        var otherKey = new SensitiveDataEncryption(Base64.getEncoder().encodeToString(new byte[32]));
        var original = record(encryptedLine(otherKey), true);

        assertThat(interceptor.intercept(original, consumerInGroup("log-consumer"))).isSameAs(original);
    }

    @Test
    void skipsRecordsWithoutTheProtectedHeader() {
        var original = record(new LogLine("order-service", "Order created", Map.of()), false);

        assertThat(interceptor.intercept(original, consumerInGroup("log-consumer"))).isSameAs(original);
    }

    @Test
    void passesNullValuesThrough() {
        // ErrorHandlingDeserializer turns unreadable records into null values
        var original = record(null, true);

        assertThat(interceptor.intercept(original, consumerInGroup("log-consumer"))).isSameAs(original);
    }
}
