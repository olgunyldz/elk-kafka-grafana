package io.github.olgunyldz.logmasking.audit;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

/**
 * One audit record (regulatory log, separate from the application logs). Answers who did what, when,
 * from where, to what, with what result, and how it correlates (PCI DSS 10.2.2, BDDK). The chain fields
 * ({@code chainId} … {@code keyId}) are filled in by {@link AuditChain}; the rest by the caller through
 * {@link AuditLogger}.
 * <p>
 * Never put card numbers, CVVs, national IDs or other personal data in here: identify people by an
 * internal customer number or token. {@link AuditLogger} rejects a record that contains such data.
 *
 * @param eventId     unique id of this record
 * @param timestamp   when it happened, UTC, ISO-8601 (servers NTP-synchronised in production, PCI 10.6)
 * @param category    CUSTOMER_CHANNEL, EMPLOYEE, SYSTEM or AUDIT_SYSTEM
 * @param action      what happened, e.g. LOGIN_FAILED, TRANSFER, CUSTOMER_RECORD_VIEWED
 * @param actor       who: customer, employee or system, by internal id
 * @param channel     mobile, internet, atm, ivr, branch, call-center, api, batch, system
 * @param source      from where: IP, device, app version
 * @param resource    on what: the affected record or object
 * @param outcome     result: SUCCESS or FAILURE, with an error code
 * @param correlation session, transaction and trace ids
 * @param details     small extra facts (amount, currency, old/new value…), checked like everything else
 * @param service     the application that wrote the record
 * @param instance    which instance of it (host name)
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record AuditEvent(
        String eventId,
        String timestamp,
        String category,
        String action,
        Actor actor,
        String channel,
        Source source,
        Resource resource,
        Outcome outcome,
        Correlation correlation,
        Map<String, String> details,
        String service,
        String instance,
        String chainId,
        Long seq,
        String prevHash,
        String keyId,
        String hash) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Actor(String type, String id, String role) {
        public static Actor customer(String id) {
            return new Actor("customer", id, null);
        }

        public static Actor employee(String id, String role) {
            return new Actor("employee", id, role);
        }

        public static Actor system(String id) {
            return new Actor("system", id, null);
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Source(String ip, String deviceId, String appVersion) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Resource(String type, String id) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Outcome(String result, String errorCode) {
        public static Outcome success() {
            return new Outcome("SUCCESS", null);
        }

        public static Outcome failure(String errorCode) {
            return new Outcome("FAILURE", errorCode);
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Correlation(String sessionId, String transactionId, String traceId) {
    }

    /** The same record with the chain fields set. */
    AuditEvent sealed(String chainId, long seq, String prevHash, String keyId, String hash) {
        return new AuditEvent(eventId, timestamp, category, action, actor, channel, source, resource, outcome,
                correlation, details, service, instance, chainId, seq, prevHash, keyId, hash);
    }
}
