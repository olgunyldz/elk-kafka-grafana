package io.github.olgunyldz.logmasking.pci.kafka;

import io.github.olgunyldz.logmasking.pci.SensitiveDataDetector;
import io.github.olgunyldz.logmasking.pci.SensitiveDataEncryption;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class PciKafkaTemplateTest {

    /** A message type like the producer's: typed fields plus a free-form payload. */
    record Event(String type, String aggregateId, Map<String, Object> payload) {
    }

    private final SensitiveDataEncryption encryption = new SensitiveDataEncryption(
            "+WpfE866Ysy6Lp51Q61eBAcupVyUtI5nnyxwm6avujc=");
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> kafka = mock(KafkaTemplate.class);
    private final PciKafkaTemplate template = new PciKafkaTemplate(
            kafka, new SensitiveDataDetector(encryption), new JsonMapper());

    @BeforeEach
    void kafkaAcceptsEverything() {
        when(kafka.send(any(ProducerRecord.class))).thenAnswer(inv -> {
            ProducerRecord<String, Object> record = inv.getArgument(0);
            var meta = new RecordMetadata(new TopicPartition(record.topic(), 0), 7, 0, 0, 0, 0);
            return CompletableFuture.completedFuture(new SendResult<>(record, meta));
        });
    }

    @SuppressWarnings("unchecked")
    private ProducerRecord<String, Object> sentRecord() {
        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafka).send(captor.capture());
        return captor.getValue();
    }

    @Test
    void sendsProtectedValueOfTheSameType() {
        var event = new Event("PaymentAuthorized", "order-42",
                Map.of("cardNumber", "4111111111111111", "cvv", "123", "password", "S3cret!", "amount", 99));

        var result = template.send("app-events", "order-42", event).join();
        var sent = sentRecord();

        assertThat(sent.value()).isInstanceOf(Event.class);
        var payload = ((Event) sent.value()).payload();
        assertThat(payload).doesNotContainKeys("cvv", "password").containsEntry("amount", 99);
        assertThat((String) payload.get("cardNumber")).startsWith("pan:v1:1111:");
        assertThat(encryption.decrypt((String) payload.get("cardNumber"))).isEqualTo("4111111111111111");
        assertThat(sent.key()).isEqualTo("order-42");
        assertThat(sent.headers().lastHeader(PciKafkaTemplate.PROTECTED_HEADER)).isNotNull();

        assertThat(result.removedValues()).containsExactlyInAnyOrder("payload.cvv (CVV)", "payload.password (credential)");
        assertThat(result.encryptedValues()).containsExactly("payload.cardNumber (card number)");
        assertThat(result.sentValue()).isSameAs(sent.value());
        assertThat(result.sendResult().getRecordMetadata().offset()).isEqualTo(7);
    }

    @Test
    void sendsCleanValuesUnchangedAndWithoutHeader() {
        var event = new Event("OrderPlaced", "order-1", Map.of("amount", 10));

        var result = template.send("app-events", "order-1", event).join();
        var sent = sentRecord();

        assertThat(sent.value()).isSameAs(event);
        assertThat(sent.headers().lastHeader(PciKafkaTemplate.PROTECTED_HEADER)).isNull();
        assertThat(result.removedValues()).isEmpty();
        assertThat(result.encryptedValues()).isEmpty();
    }

    @Test
    void keepsPartitionTimestampAndHeadersOfAFullRecord() {
        var record = new ProducerRecord<String, Object>("app-events", 2, 1_700_000_000_000L, "order-9",
                new Event("x", "order-9", Map.of("email", "jane@example.com")));
        record.headers().add("trace-id", "abc".getBytes(StandardCharsets.UTF_8));

        template.send(record).join();
        var sent = sentRecord();

        assertThat(sent.partition()).isEqualTo(2);
        assertThat(sent.timestamp()).isEqualTo(1_700_000_000_000L);
        assertThat(sent.headers().lastHeader("trace-id")).isNotNull();
        assertThat(sent.headers().lastHeader(PciKafkaTemplate.PROTECTED_HEADER)).isNotNull();
    }

    @Test
    void sendsWithoutAKey() {
        template.send("app-logs", new Event("x", "y", Map.of())).join();

        assertThat(sentRecord().key()).isNull();
    }

    @Test
    void rejectsSensitiveKeys() {
        assertThatThrownBy(() -> template.send("app-events", "4111111111111111", new Event("x", "y", Map.of())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("card number");
        verifyNoInteractions(kafka);
    }

    @Test
    void flushesTheUnderlyingTemplate() {
        template.flush();

        verify(kafka).flush();
    }
}
