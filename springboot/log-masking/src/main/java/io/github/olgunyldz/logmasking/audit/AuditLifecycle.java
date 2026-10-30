package io.github.olgunyldz.logmasking.audit;

import io.github.olgunyldz.logmasking.audit.AuditEvent.Actor;
import io.github.olgunyldz.logmasking.logging.PciLogger;
import org.slf4j.Logger;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The audit chain's own records, so that a gap in the audit log can be told apart from "nothing happened":
 * <ul>
 *   <li>{@code AUDIT_CHAIN_START} when the application starts (seq 1)</li>
 *   <li>{@code AUDIT_HEARTBEAT} every {@code audit.heartbeat-interval}: a chain without heartbeats means
 *       the audit logging stopped (the verifier reports it as STALE)</li>
 *   <li>{@code AUDIT_CHAIN_END} on a clean shutdown: the chain ended on purpose, nothing is missing at the end</li>
 * </ul>
 */
public class AuditLifecycle implements SmartLifecycle {

    private static final Logger log = PciLogger.getLogger(AuditLifecycle.class);

    private final AuditLogger audit;
    private final String service;
    private final Duration heartbeat;
    private ScheduledExecutorService scheduler;
    private volatile boolean running;

    public AuditLifecycle(AuditLogger audit, String service, Duration heartbeat) {
        this.audit = audit;
        this.service = service;
        this.heartbeat = heartbeat;
    }

    @Override
    public void start() {
        try {
            audit.start();
            log.info("Audit chain {} started", audit.chainId());
        } catch (RuntimeException e) {
            // Retried with the first audited action; until then, audited actions fail closed
            log.error("Audit chain could not be started: {}", e.getMessage());
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "audit-heartbeat");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::heartbeat, heartbeat.toMillis(), heartbeat.toMillis(), TimeUnit.MILLISECONDS);
        running = true;
    }

    private void heartbeat() {
        try {
            audit.log(AuditLogger.event(AuditLogger.AUDIT_SYSTEM, "AUDIT_HEARTBEAT")
                    .actor(Actor.system(service))
                    .channel("system")
                    .resource("audit-chain", audit.chainId())
                    .detail("lastSeq", audit.lastSeq()));
        } catch (RuntimeException e) {
            log.error("Audit heartbeat failed: {}", e.getMessage());
        }
    }

    @Override
    public void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        try {
            audit.log(AuditLogger.event(AuditLogger.AUDIT_SYSTEM, "AUDIT_CHAIN_END")
                    .actor(Actor.system(service))
                    .channel("system")
                    .resource("audit-chain", audit.chainId())
                    .detail("lastSeq", audit.lastSeq()));
        } catch (RuntimeException e) {
            log.error("Audit chain end could not be written: {}", e.getMessage());
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Start late and stop early: Kafka (the audit topic's producer) must be ready before, and still there after. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }
}
