package io.github.olgunyldz.logmasking.audit;

import io.github.olgunyldz.logmasking.audit.AuditEvent.Actor;
import io.github.olgunyldz.logmasking.audit.AuditEvent.Outcome;
import io.github.olgunyldz.logmasking.pci.SensitiveDataDetector;
import io.github.olgunyldz.logmasking.pci.SensitiveDataEncryption;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AuditLoggerTest {

    private static final String KEY = Base64.getEncoder().encodeToString(AuditVerifierTest.KEY);

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final List<String> sent = new ArrayList<>();
    private final List<String> metrics = new ArrayList<>();
    private final AuditLogger audit = new AuditLogger(kafka, new AuditChain(KEY, "audit-v1"),
            new SensitiveDataDetector(new SensitiveDataEncryption(SensitiveDataEncryption.newBase64Key())),
            "audit-events", "producer", "host-1", Duration.ofSeconds(1), (action, result) -> metrics.add(action + ":" + result));

    private void kafkaAccepts() {
        when(kafka.send(eq("audit-events"), anyString(), anyString())).thenAnswer(inv -> {
            sent.add(inv.getArgument(2));
            return CompletableFuture.completedFuture(mock(SendResult.class));
        });
    }

    private AuditLogger.Builder transfer(String customer) {
        return AuditLogger.event(AuditLogger.CUSTOMER_CHANNEL, "TRANSFER")
                .actor(Actor.customer(customer)).channel("mobile").source("10.0.0.7", "dev-42", "5.1.0")
                .resource("account", "A-1").transaction("TX-1").detail("amount", "250.00").detail("transferType", "FAST");
    }

    @Test
    void startsTheChainThenWritesTheRecord() {
        kafkaAccepts();

        AuditEvent written = audit.log(transfer("C-100234"));

        assertThat(sent).hasSize(2);
        assertThat(sent.get(0)).contains("\"action\":\"AUDIT_CHAIN_START\"", "\"seq\":1");
        assertThat(written.seq()).isEqualTo(2);
        assertThat(written.prevHash()).hasSize(64).isNotEqualTo(AuditChain.GENESIS);
        assertThat(metrics).containsExactly("AUDIT_CHAIN_START:written", "TRANSFER:written");
    }

    @Test
    void writesEveryFieldTheRegulationAsksFor() {
        kafkaAccepts();
        audit.log(transfer("C-100234").session("S-9"));

        var record = new JsonMapper().readTree(sent.get(1));
        assertThat(record.path("actor").path("type").asString()).isEqualTo("customer");     // who
        assertThat(record.path("action").asString()).isEqualTo("TRANSFER");                   // what
        assertThat(Instant.parse(record.path("timestamp").asString())).isNotNull();           // when
        assertThat(record.path("source").path("ip").asString()).isEqualTo("10.0.0.7");       // from where
        assertThat(record.path("channel").asString()).isEqualTo("mobile");
        assertThat(record.path("resource").path("id").asString()).isEqualTo("A-1");          // to what
        assertThat(record.path("outcome").path("result").asString()).isEqualTo("SUCCESS");    // result
        assertThat(record.path("correlation").path("sessionId").asString()).isEqualTo("S-9"); // correlation
        assertThat(record.path("hash").asString()).hasSize(64);
    }

    @Test
    void theWrittenRecordsVerify() {
        kafkaAccepts();
        audit.log(transfer("C-1"));
        audit.log(AuditLogger.event(AuditLogger.CUSTOMER_CHANNEL, "LOGIN_FAILED")
                .actor(Actor.customer("C-1")).channel("internet").outcome(Outcome.failure("BAD_PASSWORD")));

        var report = new AuditVerifier(Map.of("audit-v1", AuditVerifierTest.KEY))
                .verify(sent, Map.of(), Instant.now(), Duration.ofMinutes(5));
        assertThat(report.chains()).singleElement().satisfies(c -> {
            assertThat(c.status()).isEqualTo(AuditVerifier.Status.OK);
            assertThat(c.lastSeq()).isEqualTo(3);
        });
    }

    @Test
    void failsClosedAndLeavesNoGap() {
        kafkaAccepts();
        audit.log(transfer("C-1"));                                       // seq 1 (start) and 2
        when(kafka.send(eq("audit-events"), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("NOT_ENOUGH_REPLICAS")));

        assertThatThrownBy(() -> audit.log(transfer("C-2"))).isInstanceOf(AuditWriteException.class);
        assertThat(metrics).contains("TRANSFER:failed");

        kafkaAccepts();
        AuditEvent next = audit.log(transfer("C-3"));
        assertThat(next.seq()).as("the failed record took no seq").isEqualTo(3);
    }

    @Test
    void rejectsSensitiveData() {
        kafkaAccepts();

        assertThatThrownBy(() -> audit.log(transfer("10000000146")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("actor.id").hasMessageContaining("national ID");
        assertThatThrownBy(() -> audit.log(transfer("C-1").detail("note", "card 4111 1111 1111 1111")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("card number");
        assertThat(sent).as("nothing written").isEmpty();
    }
}
