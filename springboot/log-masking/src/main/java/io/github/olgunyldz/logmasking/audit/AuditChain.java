package io.github.olgunyldz.logmasking.audit;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

/**
 * A hash chain of audit records. Every record carries the hash of the one before it, and its own hash is
 * an HMAC over both:
 * <pre>hash(n) = HMAC-SHA256(key, hash(n-1) + "\n" + canonicalJson(record n without "hash"))</pre>
 * Changing, deleting, inserting or reordering a record breaks the chain from that point on; without the key
 * nobody can compute valid hashes for a forged record. One chain per application instance and start
 * ({@link #chainId()}); it begins at seq 1 with {@code prevHash} = 64 zeros.
 * <p>
 * Not thread-safe on its own: {@link AuditLogger} seals and commits one record at a time.
 */
public class AuditChain {

    public static final String GENESIS = "0".repeat(64);
    private static final JsonMapper MAPPER = new JsonMapper();

    private final String chainId;
    private final byte[] key;
    private final String keyId;
    private long seq;
    private String lastHash = GENESIS;

    /** @param base64Key a standard base64-encoded key of at least 32 bytes (audit.hmac-key) */
    public AuditChain(String base64Key, String keyId) {
        this(UUID.randomUUID().toString(), decodeKey(base64Key), keyId);
    }

    AuditChain(String chainId, byte[] key, String keyId) {
        this.chainId = chainId;
        this.key = key;
        this.keyId = keyId;
    }

    public String chainId() {
        return chainId;
    }

    public long lastSeq() {
        return seq;
    }

    /** The next link for this record, not yet part of the chain: {@link #commit} it once it's safely stored. */
    public AuditEvent seal(AuditEvent draft) {
        long next = seq + 1;
        AuditEvent unsigned = draft.sealed(chainId, next, lastHash, keyId, null);
        return draft.sealed(chainId, next, lastHash, keyId, hmac(key, lastHash, canonicalJson(unsigned)));
    }

    /** Makes a sealed record the chain's last link. */
    public void commit(AuditEvent sealed) {
        if (sealed.seq() != seq + 1 || !sealed.prevHash().equals(lastHash)) {
            throw new IllegalStateException("Out-of-order commit: seq " + sealed.seq() + " after " + seq);
        }
        seq = sealed.seq();
        lastHash = sealed.hash();
    }

    // ---- Canonical form and HMAC, shared with AuditVerifier ----

    /** The record as one line of JSON with keys sorted at every level and no "hash": what the HMAC covers. */
    public static String canonicalJson(AuditEvent event) {
        return canonicalJson(MAPPER.valueToTree(event));
    }

    /** The same, for a record read back as JSON (the verifier's side). */
    public static String canonicalJson(JsonNode record) {
        JsonNode copy = record.deepCopy();
        if (copy instanceof ObjectNode object) {
            object.remove("hash");
        }
        return MAPPER.writeValueAsString(sorted(copy));
    }

    /** The full record as it is stored: canonical JSON, including "hash". */
    public static String toJson(AuditEvent event) {
        return MAPPER.writeValueAsString(sorted(MAPPER.valueToTree(event)));
    }

    public static String hmac(byte[] key, String prevHash, String canonicalJson) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            byte[] out = mac.doFinal((prevHash + "\n" + canonicalJson).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(out);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HMAC-SHA256 not available", e);
        }
    }

    public static byte[] decodeKey(String base64Key) {
        if (base64Key == null || base64Key.isBlank()) {
            throw new IllegalStateException("audit.hmac-key is not set");
        }
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(base64Key.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("audit.hmac-key is not valid base64", e);
        }
        if (bytes.length < 32) {
            throw new IllegalStateException("audit.hmac-key must be at least 32 bytes, got " + bytes.length);
        }
        return bytes;
    }

    /** Keys sorted at every level, so the same record always gives the same bytes. Nulls are dropped. */
    private static JsonNode sorted(JsonNode node) {
        if (node instanceof ObjectNode object) {
            ObjectNode out = JsonNodeFactory.instance.objectNode();
            List<String> names = new ArrayList<>(object.propertyNames());
            names.sort(null);
            for (String name : names) {
                JsonNode child = object.get(name);
                if (child != null && !child.isNull()) {
                    out.set(name, sorted(child));
                }
            }
            return out;
        }
        if (node instanceof ArrayNode array) {
            ArrayNode out = JsonNodeFactory.instance.arrayNode();
            array.forEach(child -> out.add(sorted(child)));
            return out;
        }
        return node;
    }
}
