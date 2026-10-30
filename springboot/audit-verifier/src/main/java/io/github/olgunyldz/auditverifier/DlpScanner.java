package io.github.olgunyldz.auditverifier;

import io.github.olgunyldz.logmasking.audit.AuditEvent.Actor;
import io.github.olgunyldz.logmasking.audit.AuditEvent.Outcome;
import io.github.olgunyldz.logmasking.audit.AuditLogger;
import io.github.olgunyldz.logmasking.logging.PciLogger;
import io.github.olgunyldz.logmasking.pci.CardNumbers;
import io.github.olgunyldz.logmasking.pci.Checksums;
import io.github.olgunyldz.logmasking.pci.SensitiveDataDetector;
import io.github.olgunyldz.logmasking.pci.SensitiveDataEncryption;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * Data loss prevention scan: looks for unmasked card numbers and national IDs in everything stored in
 * Elasticsearch since the last run (application logs, infrastructure logs, Kafka messages, audit records).
 * Auditors run exactly this check ("no PAN or CVV in the logs", PCI DSS / BDDK); here it runs every minute.
 * Card numbers need the Luhn check and a card network prefix, national IDs their check digits, and
 * encrypted tokens (pan:v1:...) are skipped, so only real, readable values count.
 */
@Component
public class DlpScanner {

    public record Scan(long scanned, long findings) {
    }

    private static final Logger log = PciLogger.getLogger(DlpScanner.class);
    private static final int MAX_DOCUMENTS = 5000;

    private final RestClient elasticsearch;
    private final VerifierProperties props;
    private final AuditLogger audit;
    private Instant since = Instant.now().minusSeconds(3600);

    public DlpScanner(RestClient.Builder builder, VerifierProperties props, AuditLogger audit) {
        this.elasticsearch = builder.baseUrl(props.elasticsearchUrl()).build();
        this.props = props;
        this.audit = audit;
    }

    public Scan scan() {
        Instant until = Instant.now();
        JsonNode hits;
        try {
            hits = elasticsearch.post()
                    .uri("/{indices}/_search?ignore_unavailable=true", props.dlpIndices())
                    .body(Map.of(
                            "size", MAX_DOCUMENTS,
                            "_source", new String[]{"@timestamp", "message", "error.message", "error.stack_trace"},
                            "sort", new Object[]{Map.of("@timestamp", "asc")},
                            "query", Map.of("range", Map.of("@timestamp", Map.of("gt", since.toString(), "lte", until.toString())))))
                    .retrieve()
                    .body(JsonNode.class);
        } catch (RestClientException e) {
            log.warn("DLP scan skipped, Elasticsearch not reachable: {}", e.getMessage());
            return new Scan(0, 0);
        }
        JsonNode docs = hits.path("hits").path("hits");
        // A full page: continue after the last document next run instead of skipping the rest
        since = docs.size() == MAX_DOCUMENTS
                ? Instant.parse(docs.get(docs.size() - 1).path("_source").path("@timestamp").asString())
                : until;

        long scanned = 0, findings = 0;
        for (JsonNode hit : docs) {
            scanned++;
            String text = hit.path("_source").toString();
            String what = find(text);
            if (what != null) {
                findings++;
                String index = hit.path("_index").asString();
                String id = hit.path("_id").asString();
                // The log line itself goes through the log masking, so the value isn't repeated in clear
                log.error("DLP: unmasked {} in {} document {}", what, index, id);
                try {
                    audit.log(AuditLogger.event(AuditLogger.AUDIT_SYSTEM, "DLP_FINDING")
                            .actor(Actor.system("audit-verifier"))
                            .channel("batch")
                            .resource("elasticsearch-document", index + "/" + id)
                            .outcome(Outcome.failure("UNMASKED_" + what.toUpperCase().replace(' ', '_')))
                            .detail("index", index));
                } catch (RuntimeException e) {
                    log.error("DLP finding could not be audited: {}", e.getMessage());
                }
            }
        }
        return new Scan(scanned, findings);
    }

    /** "card number", "national ID", or null. Text inside encrypted tokens doesn't count. */
    static String find(String text) {
        String plain = SensitiveDataEncryption.TOKEN.matcher(text).replaceAll(" ");
        if (!CardNumbers.findAll(plain).isEmpty()) {
            return "card number";
        }
        Matcher m = SensitiveDataDetector.TCKN.matcher(plain);
        while (m.find()) {
            if (Checksums.tckn(m.group())) {
                return "national ID";
            }
        }
        return null;
    }
}
