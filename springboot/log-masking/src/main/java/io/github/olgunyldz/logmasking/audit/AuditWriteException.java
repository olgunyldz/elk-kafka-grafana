package io.github.olgunyldz.logmasking.audit;

/**
 * An audit record could not be written. Audit fails closed: the action that should have been audited
 * must not go ahead.
 */
public class AuditWriteException extends RuntimeException {

    public AuditWriteException(String message, Throwable cause) {
        super(message, cause);
    }

    public AuditWriteException(String message) {
        super(message);
    }
}
