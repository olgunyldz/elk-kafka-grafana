package io.github.olgunyldz.producer.api;

import io.github.olgunyldz.logmasking.audit.AuditEvent;
import io.github.olgunyldz.logmasking.audit.AuditEvent.Actor;
import io.github.olgunyldz.logmasking.audit.AuditEvent.Outcome;
import io.github.olgunyldz.logmasking.audit.AuditLogger;
import io.github.olgunyldz.logmasking.audit.AuditWriteException;
import io.github.olgunyldz.logmasking.logging.PciLogger;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.slf4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simulated customer-channel actions (mobile, internet banking, ATM, IVR). Nothing is really transferred:
 * each call writes the audit record a real banking backend would write (BDDK, PCI DSS 10.2).
 * <p>
 * Customers are identified by an internal customer number (C-100234), never a national ID: the audit
 * logger rejects a record that contains one.
 */
@RestController
@RequestMapping("/api/banking")
public class BankingController {

    private static final Logger log = PciLogger.getLogger(BankingController.class);

    /** The customer actions the regulation lists, and what each one is about. */
    public static final Map<String, String> ACTIONS = Map.ofEntries(
            Map.entry("LOGIN", "session"),
            Map.entry("LOGOUT", "session"),
            Map.entry("OTP_SENT", "otp"),
            Map.entry("OTP_VERIFIED", "otp"),
            Map.entry("DEVICE_CHANGED", "device"),
            Map.entry("SIM_CHANGE_CHECKED", "device"),
            Map.entry("PASSWORD_RESET", "credentials"),
            Map.entry("PHONE_CHANGED", "contact"),
            Map.entry("EMAIL_CHANGED", "contact"),
            Map.entry("TRANSFER", "account"),
            Map.entry("BILL_PAYMENT", "account"),
            Map.entry("CARD_LIMIT_CHANGED", "card"),
            Map.entry("LOAN_APPLICATION", "application"),
            Map.entry("CARD_APPLICATION", "application"),
            Map.entry("CONTRACT_ACCEPTED", "contract"),
            Map.entry("CONSENT_GIVEN", "consent"),
            Map.entry("CONSENT_WITHDRAWN", "consent"));

    private static final int MAX_FAILED_LOGINS = 3;

    public record ActionRequest(
            @NotBlank @Pattern(regexp = "[A-Z_]+") String action,
            @NotBlank String customerId,
            @NotBlank @Pattern(regexp = "mobile|internet|atm|ivr|call-center|branch|api") String channel,
            String deviceId,
            String appVersion,
            String sessionId,
            @Pattern(regexp = "SUCCESS|FAILURE") String outcome,
            String errorCode,
            /** Non-sensitive facts: amount, currency, transferType, newLimit… (card numbers etc. are rejected) */
            Map<String, String> details) {
    }

    public record ActionResponse(String action, String outcome, String auditEventId, String chainId, long seq,
                                 List<String> alsoAudited) {
    }

    private final AuditLogger audit;
    private final Map<String, Integer> failedLogins = new ConcurrentHashMap<>();

    public BankingController(AuditLogger audit) {
        this.audit = audit;
    }

    @GetMapping("/actions")
    public Map<String, String> actions() {
        return ACTIONS;
    }

    @PostMapping("/actions")
    public ActionResponse perform(@Valid @RequestBody ActionRequest request, HttpServletRequest http) {
        String resourceType = ACTIONS.get(request.action());
        if (resourceType == null) {
            throw new IllegalArgumentException("Unknown customer action " + request.action() + ", known: " + ACTIONS.keySet());
        }
        boolean success = !"FAILURE".equals(request.outcome());
        String action = request.action().equals("LOGIN") ? (success ? "LOGIN_SUCCESS" : "LOGIN_FAILED") : request.action();
        String transactionId = resourceType.equals("account") || resourceType.equals("card")
                ? "TX-" + UUID.randomUUID().toString().substring(0, 8) : null;

        AuditLogger.Builder record = AuditLogger.event(AuditLogger.CUSTOMER_CHANNEL, action)
                .actor(Actor.customer(request.customerId()))
                .channel(request.channel())
                .source(clientIp(http), request.deviceId(), request.appVersion())
                .resource(resourceType, resourceId(resourceType, request))
                .outcome(success ? Outcome.success() : Outcome.failure(request.errorCode() == null ? "REJECTED" : request.errorCode()))
                .session(request.sessionId())
                .transaction(transactionId);
        if (request.details() != null) {
            request.details().forEach(record::detail);
        }
        // Audit first: if it can't be written, the action doesn't happen (AuditWriteException → 503)
        AuditEvent written = audit.log(record);
        log.info("Banking action {} for customer {} on {}: {}", action, request.customerId(), request.channel(),
                written.outcome().result());

        var also = new java.util.ArrayList<String>();
        if (action.equals("LOGIN_FAILED")) {
            int failures = failedLogins.merge(request.customerId(), 1, Integer::sum);
            if (failures >= MAX_FAILED_LOGINS) {
                failedLogins.remove(request.customerId());
                audit.log(AuditLogger.event(AuditLogger.CUSTOMER_CHANNEL, "ACCOUNT_LOCKED")
                        .actor(Actor.system("producer"))
                        .channel(request.channel())
                        .source(clientIp(http), request.deviceId(), request.appVersion())
                        .resource("customer", request.customerId())
                        .session(request.sessionId())
                        .detail("reason", MAX_FAILED_LOGINS + " failed logins"));
                also.add("ACCOUNT_LOCKED");
            }
        } else if (action.equals("LOGIN_SUCCESS")) {
            failedLogins.remove(request.customerId());
        }
        return new ActionResponse(action, written.outcome().result(), written.eventId(), written.chainId(),
                written.seq(), also);
    }

    private static String resourceId(String resourceType, ActionRequest request) {
        return switch (resourceType) {
            case "session" -> request.sessionId();
            case "device" -> request.deviceId();
            default -> request.customerId();
        };
    }

    /** The client's address: the first X-Forwarded-For entry behind a proxy, else the connection's. */
    private static String clientIp(HttpServletRequest http) {
        String forwarded = http.getHeader("X-Forwarded-For");
        return forwarded != null && !forwarded.isBlank() ? forwarded.split(",")[0].trim() : http.getRemoteAddr();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail rejected(IllegalArgumentException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    /** Fail closed: the audit record couldn't be written, so the action was not carried out. */
    @ExceptionHandler(AuditWriteException.class)
    public ProblemDetail auditDown(AuditWriteException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, ex.getMessage());
    }
}
