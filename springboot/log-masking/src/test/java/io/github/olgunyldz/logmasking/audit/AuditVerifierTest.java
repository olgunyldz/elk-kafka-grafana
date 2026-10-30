package io.github.olgunyldz.logmasking.audit;

import io.github.olgunyldz.logmasking.audit.AuditEvent.Actor;
import io.github.olgunyldz.logmasking.audit.AuditVerifier.Anchor;
import io.github.olgunyldz.logmasking.audit.AuditVerifier.ChainReport;
import io.github.olgunyldz.logmasking.audit.AuditVerifier.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AuditVerifierTest {

    static final byte[] KEY = Base64.getDecoder().decode("Vb8x2lXQ7Hn3yQm1q0bC9kRzT5uW4eA6sD2fG8hJ1kM=");
    static final byte[] OTHER_KEY = new byte[32];

    private final AuditVerifier verifier = new AuditVerifier(Map.of("audit-v1", KEY));
    private List<String> lines;

    /** A chain of 6 records, as the archive stores them: start, 4 actions, a heartbeat. */
    static List<String> chain(byte[] key, String chainId, int actions) {
        var chain = new AuditChain(chainId, key, "audit-v1");
        var out = new ArrayList<String>();
        out.add(link(chain, AuditLogger.event(AuditLogger.AUDIT_SYSTEM, "AUDIT_CHAIN_START").actor(Actor.system("producer"))));
        for (int i = 0; i < actions; i++) {
            out.add(link(chain, AuditLogger.event(AuditLogger.CUSTOMER_CHANNEL, "TRANSFER")
                    .actor(Actor.customer("C-10000" + i)).channel("mobile").resource("account", "A-" + i)
                    .transaction("TX-" + i).detail("amount", 100 + i).detail("currency", "TRY")));
        }
        out.add(link(chain, AuditLogger.event(AuditLogger.AUDIT_SYSTEM, "AUDIT_HEARTBEAT").actor(Actor.system("producer"))));
        return out;
    }

    private static String link(AuditChain chain, AuditLogger.Builder builder) {
        AuditEvent sealed = chain.seal(builder.build("producer", "host-1"));
        chain.commit(sealed);
        return AuditChain.toJson(sealed);
    }

    private ChainReport only(List<String> archive, Map<String, Anchor> anchors) {
        var report = verifier.verify(archive, anchors, Instant.now(), Duration.ofMinutes(5));
        assertThat(report.chains()).hasSize(1);
        return report.chains().get(0);
    }

    @BeforeEach
    void setUp() {
        lines = chain(KEY, "chain-a", 4);
    }

    @Test
    void anUntouchedChainIsOk() {
        ChainReport r = only(lines, Map.of());

        assertThat(r.status()).isEqualTo(Status.OK);
        assertThat(r.records()).isEqualTo(6);
        assertThat(r.lastSeq()).isEqualTo(6);
    }

    @Test
    void theOrderOfLinesInTheArchiveDoesNotMatter() {
        Collections.reverse(lines);

        assertThat(only(lines, Map.of()).status()).isEqualTo(Status.OK);
    }

    @Test
    void aModifiedRecordIsDetected() {
        lines.set(2, lines.get(2).replace("\"amount\":\"101\"", "\"amount\":\"9101\""));

        ChainReport r = only(lines, Map.of());
        assertThat(r.status()).isEqualTo(Status.BROKEN);
        assertThat(r.brokenAtSeq()).isEqualTo(3);
        assertThat(r.problem()).contains("modified or forged");
    }

    @Test
    void aDeletedRecordIsDetected() {
        lines.remove(3);

        ChainReport r = only(lines, Map.of());
        assertThat(r.status()).isEqualTo(Status.BROKEN);
        assertThat(r.brokenAtSeq()).isEqualTo(4);
        assertThat(r.problem()).contains("gap: seq 4 to 4 missing");
    }

    @Test
    void aDeletedChainStartIsDetected() {
        lines.remove(0);

        assertThat(only(lines, Map.of()).problem()).contains("does not start at seq 1");
    }

    @Test
    void aForgedRecordWithoutTheKeyIsDetected() {
        // The attacker builds a complete, consistent chain, but with their own key
        lines = chain(OTHER_KEY, "chain-a", 4);

        ChainReport r = only(lines, Map.of());
        assertThat(r.status()).isEqualTo(Status.BROKEN);
        assertThat(r.brokenAtSeq()).isEqualTo(1);
    }

    @Test
    void anInsertedDuplicateIsDetected() {
        lines.add(lines.get(2));

        assertThat(only(lines, Map.of()).problem()).contains("duplicate seq 3");
    }

    @Test
    void recordsSwappedBetweenPositionsAreDetected() {
        // Give record 3 the seq of record 4 and the other way round: their prevHash links no longer fit
        String three = lines.get(2), four = lines.get(3);
        lines.set(2, four.replace("\"seq\":4", "\"seq\":3"));
        lines.set(3, three.replace("\"seq\":3", "\"seq\":4"));

        assertThat(only(lines, Map.of()).status()).isEqualTo(Status.BROKEN);
    }

    @Test
    void deletingTheLastRecordsIsDetectedByTheAnchor() {
        var first = verifier.verify(lines, Map.of(), Instant.now(), Duration.ofMinutes(5));
        Anchor anchor = first.anchors().get("chain-a");
        assertThat(anchor.seq()).isEqualTo(6);

        var truncated = new ArrayList<>(lines.subList(0, 4));          // seq 5 and 6 deleted
        ChainReport r = only(truncated, first.anchors());
        assertThat(r.status()).isEqualTo(Status.BROKEN);
        assertThat(r.problem()).contains("truncated");
    }

    @Test
    void aWholeChainDeletedIsDetectedByTheAnchor() {
        var anchors = verifier.verify(lines, Map.of(), Instant.now(), Duration.ofMinutes(5)).anchors();
        var other = chain(KEY, "chain-b", 1);

        var report = verifier.verify(other, anchors, Instant.now(), Duration.ofMinutes(5));
        assertThat(report.chains()).anySatisfy(c -> {
            assertThat(c.chainId()).isEqualTo("chain-a");
            assertThat(c.problem()).contains("all records deleted");
        });
    }

    @Test
    void aChainWithoutRecentRecordsIsStale() {
        var report = verifier.verify(lines, Map.of(), Instant.now().plus(Duration.ofMinutes(10)), Duration.ofMinutes(5));

        assertThat(report.chains().get(0).status()).isEqualTo(Status.STALE);
    }

    @Test
    void aChainClosedWithChainEndIsEnded() {
        var chain = new AuditChain("chain-c", KEY, "audit-v1");
        var archive = List.of(
                link(chain, AuditLogger.event(AuditLogger.AUDIT_SYSTEM, "AUDIT_CHAIN_START").actor(Actor.system("p"))),
                link(chain, AuditLogger.event(AuditLogger.AUDIT_SYSTEM, "AUDIT_CHAIN_END").actor(Actor.system("p"))));

        var report = verifier.verify(archive, Map.of(), Instant.now().plus(Duration.ofDays(1)), Duration.ofMinutes(5));
        assertThat(report.chains().get(0).status()).isEqualTo(Status.ENDED);
    }

    @Test
    void aRotatedKeyStillVerifiesOldRecords() {
        var rotated = new AuditVerifier(Map.of("audit-v1", KEY, "audit-v2", OTHER_KEY));

        assertThat(rotated.verify(lines, Map.of(), Instant.now(), Duration.ofMinutes(5)).chains().get(0).status())
                .isEqualTo(Status.OK);
    }

    @Test
    void unreadableLinesAreReported() {
        lines.add("not json at all");

        var report = verifier.verify(lines, Map.of(), Instant.now(), Duration.ofMinutes(5));
        assertThat(report.unreadableLines()).hasSize(1);
        assertThat(report.broken()).isEqualTo(1);
    }
}
