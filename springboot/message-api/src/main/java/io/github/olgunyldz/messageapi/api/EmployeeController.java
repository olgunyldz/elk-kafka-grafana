package io.github.olgunyldz.messageapi.api;

import io.github.olgunyldz.logmasking.audit.AuditEvent;
import io.github.olgunyldz.logmasking.audit.AuditEvent.Actor;
import io.github.olgunyldz.logmasking.audit.AuditEvent.Outcome;
import io.github.olgunyldz.logmasking.audit.AuditLogger;
import io.github.olgunyldz.logmasking.audit.AuditWriteException;
import io.github.olgunyldz.logmasking.logging.PciLogger;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.slf4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Simulated employee and back-office actions. Each one writes the audit record a bank's internal
 * systems would write (BDDK: staff viewing customer data is what auditors ask about most).
 * The employee comes from the X-Employee-Id / X-Employee-Role headers (an SSO token in a real system),
 * the channel from X-Channel (branch, call-center, back-office).
 * <p>
 * CORS: the producer's web page (localhost:8081) calls these endpoints from the browser.
 */
@RestController
@CrossOrigin(origins = {"http://localhost:8081", "http://127.0.0.1:8081"})
public class EmployeeController {

    private static final Logger log = PciLogger.getLogger(EmployeeController.class);

    public record RoleChange(@NotBlank String userId, @NotBlank String newRole, String approvedBy) {
    }

    public record ConfigChange(@NotBlank String parameter, @NotBlank String oldValue, @NotBlank String newValue,
                               String approvedBy) {
    }

    public record Audited(String action, String outcome, String errorCode, String auditEventId, String chainId,
                          long seq, Object data) {
        static Audited of(AuditEvent e, Object data) {
            return new Audited(e.action(), e.outcome().result(), e.outcome().errorCode(), e.eventId(), e.chainId(),
                    e.seq(), data);
        }
    }

    private final AuditLogger audit;

    public EmployeeController(AuditLogger audit) {
        this.audit = audit;
    }

    /** A staff member opens a customer's screen. */
    @GetMapping("/api/customers/{customerId}")
    public ResponseEntity<Audited> viewCustomer(@PathVariable String customerId,
                                                @RequestHeader(value = "X-Employee-Id", required = false) String employee,
                                                @RequestHeader(value = "X-Employee-Role", required = false) String role,
                                                @RequestHeader(value = "X-Channel", defaultValue = "branch") String channel,
                                                HttpServletRequest http) {
        if (employee == null || employee.isBlank()) {
            return denied("CUSTOMER_RECORD_VIEWED", "customer", customerId, channel, http, "NO_EMPLOYEE_ID");
        }
        AuditEvent e = audit.log(event(AuditLogger.EMPLOYEE, "CUSTOMER_RECORD_VIEWED", employee, role, channel, http)
                .resource("customer", customerId)
                .detail("screen", "customer-summary"));
        log.info("Employee {} viewed customer {}", employee, customerId);
        // What the screen shows is already masked: the record holds no personal data in clear
        var profile = Map.of("customerId", customerId, "name", "A*** Y***", "segment", "retail",
                "phone", "+** *** *** ** 67", "products", 3);
        return ResponseEntity.ok(Audited.of(e, profile));
    }

    /** Bulk query and export of customer data: reports, screen exports. */
    @GetMapping("/api/customers/export")
    public ResponseEntity<Audited> exportCustomers(@RequestParam(defaultValue = "retail") String segment,
                                                   @RequestParam(defaultValue = "500") int rows,
                                                   @RequestParam(defaultValue = "csv") String format,
                                                   @RequestHeader(value = "X-Employee-Id", required = false) String employee,
                                                   @RequestHeader(value = "X-Employee-Role", required = false) String role,
                                                   @RequestHeader(value = "X-Channel", defaultValue = "back-office") String channel,
                                                   HttpServletRequest http) {
        if (employee == null || employee.isBlank()) {
            return denied("DATA_EXPORTED", "customer-segment", segment, channel, http, "NO_EMPLOYEE_ID");
        }
        AuditEvent e = audit.log(event(AuditLogger.EMPLOYEE, "DATA_EXPORTED", employee, role, channel, http)
                .resource("customer-segment", segment)
                .detail("rows", rows).detail("format", format));
        return ResponseEntity.ok(Audited.of(e, Map.of("segment", segment, "rows", rows, "format", format)));
    }

    /** Role and permission changes need a second person's approval (four eyes); the approver is audited too. */
    @PostMapping("/api/admin/roles")
    public ResponseEntity<Audited> changeRole(@Valid @RequestBody RoleChange change,
                                              @RequestHeader(value = "X-Employee-Id", required = false) String employee,
                                              @RequestHeader(value = "X-Employee-Role", required = false) String role,
                                              HttpServletRequest http) {
        return fourEyes("ROLE_CHANGED", "user", change.userId(), change.approvedBy(), employee, role, http,
                Map.of("newRole", change.newRole()));
    }

    /** Configuration and parameter changes: interest, limit and fee tables. */
    @PostMapping("/api/admin/config")
    public ResponseEntity<Audited> changeConfig(@Valid @RequestBody ConfigChange change,
                                                @RequestHeader(value = "X-Employee-Id", required = false) String employee,
                                                @RequestHeader(value = "X-Employee-Role", required = false) String role,
                                                HttpServletRequest http) {
        return fourEyes("CONFIG_CHANGED", "parameter", change.parameter(), change.approvedBy(), employee, role, http,
                Map.of("oldValue", change.oldValue(), "newValue", change.newValue()));
    }

    private ResponseEntity<Audited> fourEyes(String action, String resourceType, String resourceId, String approvedBy,
                                             String employee, String role, HttpServletRequest http,
                                             Map<String, String> details) {
        if (employee == null || employee.isBlank()) {
            return denied(action, resourceType, resourceId, "back-office", http, "NO_EMPLOYEE_ID");
        }
        boolean approved = approvedBy != null && !approvedBy.isBlank() && !approvedBy.equals(employee);
        var record = event(AuditLogger.EMPLOYEE, action, employee, role, "back-office", http)
                .resource(resourceType, resourceId)
                .outcome(approved ? Outcome.success() : Outcome.failure("FOUR_EYES_REQUIRED"))
                .detail("approvedBy", approvedBy);
        details.forEach(record::detail);
        AuditEvent e = audit.log(record);
        return ResponseEntity.status(approved ? HttpStatus.OK : HttpStatus.FORBIDDEN).body(Audited.of(e, details));
    }

    private ResponseEntity<Audited> denied(String action, String resourceType, String resourceId, String channel,
                                           HttpServletRequest http, String code) {
        AuditEvent e = audit.log(AuditLogger.event(AuditLogger.EMPLOYEE, action)
                .actor(new Actor("unknown", "anonymous", null))
                .channel(channel)
                .source(clientIp(http), null, null)
                .resource(resourceType, resourceId)
                .outcome(Outcome.failure(code)));
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Audited.of(e, null));
    }

    private static AuditLogger.Builder event(String category, String action, String employee, String role,
                                             String channel, HttpServletRequest http) {
        return AuditLogger.event(category, action)
                .actor(Actor.employee(employee, role))
                .channel(channel)
                .source(clientIp(http), http.getHeader("X-Device-Id"), null)
                .session(http.getHeader("X-Session-Id"));
    }

    private static String clientIp(HttpServletRequest http) {
        String forwarded = http.getHeader("X-Forwarded-For");
        return forwarded != null && !forwarded.isBlank() ? forwarded.split(",")[0].trim() : http.getRemoteAddr();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail rejected(IllegalArgumentException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    @ExceptionHandler(AuditWriteException.class)
    public ProblemDetail auditDown(AuditWriteException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, ex.getMessage());
    }
}
