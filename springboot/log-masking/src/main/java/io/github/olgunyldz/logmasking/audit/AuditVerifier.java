package io.github.olgunyldz.logmasking.audit;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Checks audit chains as they were archived (one JSON record per line). For every chain:
 * <ul>
 *   <li>it starts at seq 1 with AUDIT_CHAIN_START and the genesis prevHash (nothing deleted at the start)</li>
 *   <li>seq goes up by one, with no gaps and no duplicates (nothing deleted or inserted)</li>
 *   <li>every prevHash is the previous record's hash (nothing reordered or replaced)</li>
 *   <li>every hash is the HMAC of the record with a known key (nothing modified, nothing forged)</li>
 *   <li>it reaches at least the last anchor, with the same hash there (nothing deleted at the end,
 *       nothing rewritten that was verified before)</li>
 * </ul>
 * A chain that passes is OK, ENDED (it closed with AUDIT_CHAIN_END) or STALE (no record for longer than
 * {@code staleAfter} and not ended: the audit logging stopped, or the app died). Only the first break per
 * chain is reported: everything after it is untrustworthy anyway.
 */
public class AuditVerifier {

    public enum Status { OK, ENDED, STALE, BROKEN }

    /** The last verified point of a chain, kept separately from the archive. */
    public record Anchor(String chainId, long seq, String hash, String verifiedAt) {
    }

    public record ChainReport(String chainId, String service, String instance, long records, long lastSeq,
                              String lastTimestamp, Status status, String problem, Long brokenAtSeq) {
    }

    public record Report(List<ChainReport> chains, List<String> unreadableLines, Map<String, Anchor> anchors) {
        public long broken() {
            return chains.stream().filter(c -> c.status() == Status.BROKEN).count() + (unreadableLines.isEmpty() ? 0 : 1);
        }

        public long stale() {
            return chains.stream().filter(c -> c.status() == Status.STALE).count();
        }
    }

    private static final JsonMapper MAPPER = new JsonMapper();

    private final Map<String, byte[]> keys;

    /** @param keys HMAC keys by key id, e.g. {"audit-v1": key}; old ids stay after a rotation */
    public AuditVerifier(Map<String, byte[]> keys) {
        this.keys = Map.copyOf(keys);
    }

    public Report verify(List<String> lines, Map<String, Anchor> anchors, Instant now, Duration staleAfter) {
        Map<String, List<JsonNode>> byChain = new LinkedHashMap<>();
        List<String> unreadable = new ArrayList<>();
        for (String line : lines) {
            if (line.isBlank()) continue;
            try {
                JsonNode record = MAPPER.readTree(line);
                String chainId = record.path("chainId").asString(null);
                if (chainId == null) {
                    unreadable.add(line);
                } else {
                    byChain.computeIfAbsent(chainId, k -> new ArrayList<>()).add(record);
                }
            } catch (RuntimeException e) {
                unreadable.add(line);
            }
        }

        List<ChainReport> reports = new ArrayList<>();
        Map<String, Anchor> newAnchors = new LinkedHashMap<>(anchors);
        for (var entry : byChain.entrySet()) {
            ChainReport report = verifyChain(entry.getKey(), entry.getValue(), anchors.get(entry.getKey()), now, staleAfter);
            reports.add(report);
            if (report.status() != Status.BROKEN) {
                JsonNode last = entry.getValue().get(entry.getValue().size() - 1);
                newAnchors.put(entry.getKey(), new Anchor(entry.getKey(), report.lastSeq(),
                        last.path("hash").asString(), now.toString()));
            }
        }
        // A chain that has an anchor but no records at all any more: deleted completely
        for (Anchor anchor : anchors.values()) {
            if (!byChain.containsKey(anchor.chainId())) {
                reports.add(new ChainReport(anchor.chainId(), null, null, 0, 0, null, Status.BROKEN,
                        "all records deleted: anchor at seq " + anchor.seq(), 1L));
            }
        }
        return new Report(reports, unreadable, newAnchors);
    }

    private ChainReport verifyChain(String chainId, List<JsonNode> records, Anchor anchor, Instant now, Duration staleAfter) {
        records.sort(Comparator.comparingLong(r -> r.path("seq").asLong()));
        JsonNode first = records.get(0);
        String service = first.path("service").asString(null);
        String instance = first.path("instance").asString(null);
        String prevHash = AuditChain.GENESIS;
        long expected = 1;

        for (JsonNode record : records) {
            long seq = record.path("seq").asLong();
            if (seq < expected) {
                return broken(chainId, service, instance, records, "duplicate seq " + seq + " (inserted or replayed record)", seq);
            }
            if (seq > expected) {
                return broken(chainId, service, instance, records,
                        (expected == 1 ? "chain does not start at seq 1" : "gap: seq " + expected + " to " + (seq - 1) + " missing")
                                + " (deleted records)", expected);
            }
            if (seq == 1 && !"AUDIT_CHAIN_START".equals(record.path("action").asString())) {
                return broken(chainId, service, instance, records, "seq 1 is not AUDIT_CHAIN_START", 1L);
            }
            if (!prevHash.equals(record.path("prevHash").asString())) {
                return broken(chainId, service, instance, records, "prevHash does not match seq " + (seq - 1) + "'s hash (reordered or replaced)", seq);
            }
            byte[] key = keys.get(record.path("keyId").asString());
            if (key == null) {
                return broken(chainId, service, instance, records, "unknown keyId '" + record.path("keyId").asString() + "'", seq);
            }
            String hash = record.path("hash").asString();
            if (!AuditChain.hmac(key, prevHash, AuditChain.canonicalJson(record)).equals(hash)) {
                return broken(chainId, service, instance, records, "hash does not match the record (modified or forged)", seq);
            }
            if (anchor != null && seq == anchor.seq() && !hash.equals(anchor.hash())) {
                return broken(chainId, service, instance, records, "differs from the anchor at seq " + seq + " (rewritten after verification)", seq);
            }
            prevHash = hash;
            expected = seq + 1;
        }

        long lastSeq = expected - 1;
        JsonNode last = records.get(records.size() - 1);
        String lastTime = last.path("timestamp").asString(null);
        if (anchor != null && lastSeq < anchor.seq()) {
            return new ChainReport(chainId, service, instance, records.size(), lastSeq, lastTime, Status.BROKEN,
                    "truncated: ends at seq " + lastSeq + ", anchor says " + anchor.seq() + " (records deleted at the end)", lastSeq + 1);
        }
        if ("AUDIT_CHAIN_END".equals(last.path("action").asString())) {
            return new ChainReport(chainId, service, instance, records.size(), lastSeq, lastTime, Status.ENDED, null, null);
        }
        if (lastTime != null && Instant.parse(lastTime).isBefore(now.minus(staleAfter))) {
            return new ChainReport(chainId, service, instance, records.size(), lastSeq, lastTime, Status.STALE,
                    "no record since " + lastTime + " and no AUDIT_CHAIN_END (audit logging stopped?)", null);
        }
        return new ChainReport(chainId, service, instance, records.size(), lastSeq, lastTime, Status.OK, null, null);
    }

    private static ChainReport broken(String chainId, String service, String instance, List<JsonNode> records,
                                      String problem, long seq) {
        return new ChainReport(chainId, service, instance, records.size(), 0,
                records.get(records.size() - 1).path("timestamp").asString(null), Status.BROKEN, problem, seq);
    }
}
