package io.github.olgunyldz.auditverifier;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Map;

/**
 * @param archiveDir       the append-only audit archive written by Logstash (audit-YYYY-MM-DD.jsonl)
 * @param anchorsFile      where the last verified point of every chain is kept, outside the archive
 * @param keys             HMAC keys by key id (base64); keep old ids after a rotation
 * @param staleAfter       a chain with no record for this long, and no AUDIT_CHAIN_END, is STALE
 * @param elasticsearchUrl for the DLP scan
 * @param dlpIndices       indices the DLP scan searches for unmasked card numbers and national IDs
 */
@ConfigurationProperties("verifier")
public record VerifierProperties(String archiveDir, String anchorsFile, Map<String, String> keys, Duration staleAfter,
                                 String elasticsearchUrl, String dlpIndices) {
}
