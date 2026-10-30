package io.github.olgunyldz.auditverifier;

import io.github.olgunyldz.logmasking.audit.AuditEvent.Actor;
import io.github.olgunyldz.logmasking.audit.AuditLogger;
import io.github.olgunyldz.logmasking.audit.AuditVerifier;
import io.github.olgunyldz.logmasking.audit.AuditVerifier.Anchor;
import io.github.olgunyldz.logmasking.audit.AuditVerifier.ChainReport;
import io.github.olgunyldz.logmasking.audit.AuditVerifier.Status;
import io.github.olgunyldz.logmasking.logging.PciLogger;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Every minute: reads the audit archive, checks every chain, keeps the anchors up to date, and reports.
 * Its findings are audit records themselves (AUDIT_CHAIN_BROKEN, AUDIT_LOG_INTERRUPTED,
 * AUDIT_VERIFICATION_COMPLETED): the log system's own events belong in the audit trail.
 */
@Component
public class ChainVerificationJob {

    private static final Logger log = PciLogger.getLogger(ChainVerificationJob.class);
    private static final JsonMapper MAPPER = new JsonMapper();

    private final VerifierProperties props;
    private final AuditVerifier verifier;
    private final AuditLogger audit;
    private final DlpScanner dlp;
    private final Set<String> reportedBroken = new HashSet<>();
    private final Set<String> reportedStale = new HashSet<>();

    private final AtomicLong chainsOk = new AtomicLong(), chainsEnded = new AtomicLong(),
            chainsStale = new AtomicLong(), chainsBroken = new AtomicLong(), recordsVerified = new AtomicLong(),
            lastRun = new AtomicLong(), dlpFindings = new AtomicLong(), dlpScanned = new AtomicLong();

    public ChainVerificationJob(VerifierProperties props, AuditLogger audit, DlpScanner dlp, MeterRegistry registry) {
        this.props = props;
        this.audit = audit;
        this.dlp = dlp;
        Map<String, byte[]> keys = new LinkedHashMap<>();
        props.keys().forEach((id, key) -> keys.put(id, Base64.getDecoder().decode(key.trim())));
        this.verifier = new AuditVerifier(keys);

        Gauge.builder("audit.verifier.chains", chainsOk, AtomicLong::get).tag("status", "ok").register(registry);
        Gauge.builder("audit.verifier.chains", chainsEnded, AtomicLong::get).tag("status", "ended").register(registry);
        Gauge.builder("audit.verifier.chains", chainsStale, AtomicLong::get).tag("status", "stale").register(registry);
        Gauge.builder("audit.verifier.chains", chainsBroken, AtomicLong::get).tag("status", "broken").register(registry);
        Gauge.builder("audit.verifier.records.verified", recordsVerified, AtomicLong::get).register(registry);
        Gauge.builder("audit.verifier.last.run.seconds", lastRun, AtomicLong::get)
                .description("Unix time of the last completed verification").register(registry);
        Gauge.builder("audit.dlp.findings", dlpFindings, AtomicLong::get)
                .description("Unmasked card numbers / national IDs found by the last DLP scan").register(registry);
        Gauge.builder("audit.dlp.documents.scanned", dlpScanned, AtomicLong::get).register(registry);
    }

    @Scheduled(initialDelayString = "${verifier.initial-delay:30s}", fixedDelayString = "${verifier.interval:60s}")
    public void run() {
        Instant now = Instant.now();
        List<String> lines = readArchive();
        if (lines == null) {
            // Not "0 chains, all fine": an archive nobody can read is an audit failure
            chainsBroken.set(1);
            lastRun.set(now.getEpochSecond());
            return;
        }
        var report = verifier.verify(lines, readAnchors(), now, props.staleAfter());
        writeAnchors(report.anchors());

        long ok = 0, ended = 0, stale = 0, broken = 0, records = 0;
        for (ChainReport c : report.chains()) {
            records += c.records();
            switch (c.status()) {
                case OK -> {
                    ok++;
                    log.info("Audit chain OK: {} ({}/{}) seq 1..{}", c.chainId(), c.service(), c.instance(), c.lastSeq());
                }
                case ENDED -> ended++;
                case STALE -> {
                    stale++;
                    log.warn("Audit chain STALE: {} ({}/{}) {}", c.chainId(), c.service(), c.instance(), c.problem());
                    if (reportedStale.add(c.chainId())) {
                        record("AUDIT_LOG_INTERRUPTED", c);
                    }
                }
                case BROKEN -> {
                    broken++;
                    log.error("Audit chain BROKEN: {} ({}/{}) at seq {}: {}", c.chainId(), c.service(), c.instance(),
                            c.brokenAtSeq(), c.problem());
                    if (reportedBroken.add(c.chainId())) {
                        record("AUDIT_CHAIN_BROKEN", c);
                    }
                }
            }
        }
        if (!report.unreadableLines().isEmpty()) {
            log.error("Audit archive has {} unreadable line(s): a record was damaged or something else was written",
                    report.unreadableLines().size());
            broken++;
        }

        var scan = dlp.scan();
        dlpFindings.set(scan.findings());
        dlpScanned.set(scan.scanned());

        chainsOk.set(ok);
        chainsEnded.set(ended);
        chainsStale.set(stale);
        chainsBroken.set(broken);
        recordsVerified.set(records);
        lastRun.set(now.getEpochSecond());
        log.info("Audit verification: {} chains ({} ok, {} ended, {} stale, {} broken), {} records; DLP: {} documents, {} findings",
                report.chains().size(), ok, ended, stale, broken, records, scan.scanned(), scan.findings());
        try {
            audit.log(AuditLogger.event(AuditLogger.AUDIT_SYSTEM, "AUDIT_VERIFICATION_COMPLETED")
                    .actor(Actor.system("audit-verifier"))
                    .channel("batch")
                    .resource("audit-archive", props.archiveDir())
                    .outcome(broken == 0 && scan.findings() == 0
                            ? io.github.olgunyldz.logmasking.audit.AuditEvent.Outcome.success()
                            : io.github.olgunyldz.logmasking.audit.AuditEvent.Outcome.failure(broken > 0 ? "CHAIN_BROKEN" : "DLP_FINDINGS"))
                    .detail("chains", report.chains().size()).detail("broken", broken).detail("stale", stale)
                    .detail("records", records).detail("dlpScanned", scan.scanned()).detail("dlpFindings", scan.findings()));
        } catch (RuntimeException e) {
            log.error("Verification result could not be audited: {}", e.getMessage());
        }
    }

    private void record(String action, ChainReport c) {
        try {
            audit.log(AuditLogger.event(AuditLogger.AUDIT_SYSTEM, action)
                    .actor(Actor.system("audit-verifier"))
                    .channel("batch")
                    .resource("audit-chain", c.chainId())
                    .outcome(io.github.olgunyldz.logmasking.audit.AuditEvent.Outcome.failure(c.status().name()))
                    .detail("service", c.service()).detail("instance", c.instance())
                    .detail("seq", c.brokenAtSeq() != null ? c.brokenAtSeq() : c.lastSeq())
                    .detail("problem", c.problem()));
        } catch (RuntimeException e) {
            log.error("{} could not be audited: {}", action, e.getMessage());
        }
    }

    /** Every line of every archive file, or null when the archive can't be read. */
    private List<String> readArchive() {
        Path dir = Path.of(props.archiveDir());
        List<String> lines = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path file : files.filter(f -> f.getFileName().toString().endsWith(".jsonl")).sorted().toList()) {
                lines.addAll(Files.readAllLines(file, StandardCharsets.UTF_8));
            }
            return lines;
        } catch (IOException e) {
            log.error("Audit archive could not be read, verification failed: {}", e.toString());
            try {
                audit.log(AuditLogger.event(AuditLogger.AUDIT_SYSTEM, "AUDIT_ARCHIVE_UNREADABLE")
                        .actor(Actor.system("audit-verifier"))
                        .channel("batch")
                        .resource("audit-archive", props.archiveDir())
                        .outcome(io.github.olgunyldz.logmasking.audit.AuditEvent.Outcome.failure("ARCHIVE_UNREADABLE"))
                        .detail("problem", e.getClass().getSimpleName()));
            } catch (RuntimeException ex) {
                log.error("AUDIT_ARCHIVE_UNREADABLE could not be audited: {}", ex.getMessage());
            }
            return null;
        }
    }

    private Map<String, Anchor> readAnchors() {
        Path file = Path.of(props.anchorsFile());
        if (!Files.exists(file)) {
            return Map.of();
        }
        try {
            return MAPPER.readValue(file.toFile(), new TypeReference<Map<String, Anchor>>() { });
        } catch (RuntimeException e) {
            log.error("Anchors file {} could not be read: {}", file, e.getMessage());
            return Map.of();
        }
    }

    private void writeAnchors(Map<String, Anchor> anchors) {
        Path file = Path.of(props.anchorsFile());
        try {
            Files.createDirectories(file.getParent());
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), anchors);
            Files.move(tmp, file, java.nio.file.StandardCopyOption.REPLACE_EXISTING, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException | RuntimeException e) {
            log.error("Anchors could not be written: {}", e.getMessage());
        }
    }
}
