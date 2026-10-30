package io.github.olgunyldz.logmasking.audit;

import io.github.olgunyldz.logmasking.audit.AuditEvent.Actor;
import io.github.olgunyldz.logmasking.audit.AuditEvent.Correlation;
import io.github.olgunyldz.logmasking.audit.AuditEvent.Outcome;
import io.github.olgunyldz.logmasking.audit.AuditEvent.Resource;
import io.github.olgunyldz.logmasking.audit.AuditEvent.Source;
import io.github.olgunyldz.logmasking.logging.PciLogger;
import io.github.olgunyldz.logmasking.pci.SensitiveDataDetector;
import org.slf4j.Logger;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Writes audit records: checks them, links them into this instance's {@link AuditChain}, and sends them to
 * the audit topic, waiting until Kafka has them (acks=all; the topic has min.insync.replicas=2).
 * <p>
 * <b>Fails closed:</b> if a record can't be written, {@link #log} throws {@link AuditWriteException} and the
 * caller must not carry out the action. The chain only moves on once a record is stored, so a failed write
 * leaves no gap. One record at a time, so the chain stays in order.
 * <p>
 * <b>No sensitive data:</b> a record whose fields contain a card number, national ID, IBAN, email or phone
 * number is rejected with {@link IllegalArgumentException}: identify people by internal ids, not by their data.
 */
public class AuditLogger {

    public static final String CUSTOMER_CHANNEL = "CUSTOMER_CHANNEL";
    public static final String EMPLOYEE = "EMPLOYEE";
    public static final String SYSTEM = "SYSTEM";
    public static final String AUDIT_SYSTEM = "AUDIT_SYSTEM";

    private static final Logger log = PciLogger.getLogger(AuditLogger.class);

    private final KafkaTemplate<String, String> kafka;
    private final AuditChain chain;
    private final SensitiveDataDetector detector;
    private final String topic;
    private final String service;
    private final String instance;
    private final Duration sendTimeout;
    /** (action, result) → called after every attempt; result is "written" or "failed". For metrics. */
    private final BiConsumer<String, String> listener;

    public AuditLogger(KafkaTemplate<String, String> kafka, AuditChain chain, SensitiveDataDetector detector,
                       String topic, String service, String instance, Duration sendTimeout,
                       BiConsumer<String, String> listener) {
        this.kafka = kafka;
        this.chain = chain;
        this.detector = detector;
        this.topic = topic;
        this.service = service;
        this.instance = instance;
        this.sendTimeout = sendTimeout;
        this.listener = listener;
    }

    public static Builder event(String category, String action) {
        return new Builder(category, action);
    }

    /**
     * Writes the record and returns it as stored (with chain fields).
     *
     * @throws IllegalArgumentException if a field contains sensitive data (nothing is written)
     * @throws AuditWriteException      if the record could not be stored: don't carry out the action
     */
    public synchronized AuditEvent log(Builder builder) {
        AuditEvent draft = builder.build(service, instance);
        rejectSensitiveData(draft);
        ensureStarted(builder.action);
        return write(draft);
    }

    /** Starts the chain (AUDIT_CHAIN_START) if it hasn't started yet. */
    public synchronized void start() {
        ensureStarted(null);
    }

    public String chainId() {
        return chain.chainId();
    }

    public long lastSeq() {
        return chain.lastSeq();
    }

    private void ensureStarted(String action) {
        if (chain.lastSeq() == 0 && !"AUDIT_CHAIN_START".equals(action)) {
            write(event(AUDIT_SYSTEM, "AUDIT_CHAIN_START")
                    .actor(Actor.system(service))
                    .channel("system")
                    .resource("audit-chain", chain.chainId())
                    .outcome(Outcome.success())
                    .build(service, instance));
        }
    }

    private AuditEvent write(AuditEvent draft) {
        AuditEvent sealed = chain.seal(draft);
        try {
            kafka.send(topic, sealed.chainId(), AuditChain.toJson(sealed))
                    .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(sealed, e);
        } catch (Exception e) {
            fail(sealed, e);
        }
        chain.commit(sealed);
        listener.accept(sealed.action(), "written");
        return sealed;
    }

    private void fail(AuditEvent sealed, Exception e) {
        listener.accept(sealed.action(), "failed");
        log.error("Audit record {} (chain {} seq {}) could not be written: {}",
                sealed.action(), sealed.chainId(), sealed.seq(), e.getMessage());
        throw new AuditWriteException("Audit record could not be written; the action is not allowed", e);
    }

    /** Checks every caller-provided value; the generated ids, times and chain fields are left out. */
    private void rejectSensitiveData(AuditEvent e) {
        var values = new LinkedHashMap<String, String>();
        values.put("action", e.action());
        values.put("channel", e.channel());
        if (e.actor() != null) {
            values.put("actor.id", e.actor().id());
            values.put("actor.role", e.actor().role());
        }
        if (e.source() != null) {
            values.put("source.deviceId", e.source().deviceId());
            values.put("source.appVersion", e.source().appVersion());
        }
        if (e.resource() != null) {
            values.put("resource.id", e.resource().id());
        }
        if (e.outcome() != null) {
            values.put("outcome.errorCode", e.outcome().errorCode());
        }
        if (e.correlation() != null) {
            values.put("correlation.sessionId", e.correlation().sessionId());
            values.put("correlation.transactionId", e.correlation().transactionId());
        }
        if (e.details() != null) {
            e.details().forEach((k, v) -> values.put("details." + k, v));
        }
        values.forEach((field, value) -> {
            if (value == null) return;
            List<String> found = detector.findInText(value);
            if (!found.isEmpty()) {
                throw new IllegalArgumentException("Audit record rejected: " + field + " contains "
                        + String.join(", ", found) + ". Use an internal id or token instead.");
            }
        });
    }

    /** Collects the caller's part of a record. */
    public static final class Builder {
        private final String category;
        private final String action;
        private Actor actor;
        private String channel;
        private Source source;
        private Resource resource;
        private Outcome outcome = Outcome.success();
        private String sessionId;
        private String transactionId;
        private final Map<String, String> details = new LinkedHashMap<>();

        private Builder(String category, String action) {
            this.category = category;
            this.action = action;
        }

        public Builder actor(Actor actor) {
            this.actor = actor;
            return this;
        }

        public Builder channel(String channel) {
            this.channel = channel;
            return this;
        }

        public Builder source(String ip, String deviceId, String appVersion) {
            this.source = new Source(ip, deviceId, appVersion);
            return this;
        }

        public Builder resource(String type, String id) {
            this.resource = new Resource(type, id);
            return this;
        }

        public Builder outcome(Outcome outcome) {
            this.outcome = outcome;
            return this;
        }

        public Builder session(String sessionId) {
            this.sessionId = sessionId;
            return this;
        }

        public Builder transaction(String transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public Builder detail(String key, Object value) {
            if (value != null) {
                details.put(key, String.valueOf(value));
            }
            return this;
        }

        public String action() {
            return action;
        }

        AuditEvent build(String service, String instance) {
            String traceId = MDC.get("traceId");      // set by Micrometer Tracing: links the record to the trace
            Correlation correlation = sessionId == null && transactionId == null && traceId == null
                    ? null : new Correlation(sessionId, transactionId, traceId);
            return new AuditEvent(UUID.randomUUID().toString(), Instant.now().truncatedTo(ChronoUnit.MILLIS).toString(),
                    category, action, actor, channel, source, resource, outcome, correlation,
                    details.isEmpty() ? null : Map.copyOf(details), service, instance,
                    null, null, null, null, null);
        }
    }
}
