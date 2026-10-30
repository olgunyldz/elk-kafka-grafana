package io.github.olgunyldz.logmasking.logging;

import java.util.Objects;

/**
 * Exceptions with sensitive data in their message, e.g. {@code IllegalArgumentException("invalid card 4111...")}.
 * A Throwable's message can't be changed, so a copy is logged instead: a {@link MaskedException} whose message
 * names the original type, with the same stack trace, masked cause and masked suppressed exceptions.
 * Exceptions without sensitive data are logged as they are.
 */
final class MaskedThrowables {

    private static final int MAX_DEPTH = 50;

    private MaskedThrowables() {
    }

    /** The exception itself if nothing in its chain needs masking, otherwise a masked copy. */
    static Throwable mask(Throwable t, SensitiveDataMasker masker) {
        return t == null || !needsMasking(t, masker, 0) ? t : copy(t, masker, 0);
    }

    private static boolean needsMasking(Throwable t, SensitiveDataMasker masker, int depth) {
        if (t == null || depth > MAX_DEPTH) {
            return false;
        }
        String message = t.getMessage();
        if (message != null && !Objects.equals(message, masker.mask(message))) {
            return true;
        }
        for (Throwable s : t.getSuppressed()) {
            if (needsMasking(s, masker, depth + 1)) {
                return true;
            }
        }
        return t.getCause() != t && needsMasking(t.getCause(), masker, depth + 1);
    }

    private static Throwable copy(Throwable t, SensitiveDataMasker masker, int depth) {
        String message = masker.mask(t.getMessage());
        Throwable cause = t.getCause() != null && t.getCause() != t && depth < MAX_DEPTH
                ? copy(t.getCause(), masker, depth + 1) : null;
        MaskedException copy = new MaskedException(
                t.getClass().getName() + (message != null ? ": " + message : ""), cause);
        copy.setStackTrace(t.getStackTrace());
        for (Throwable s : t.getSuppressed()) {
            if (depth < MAX_DEPTH) {
                copy.addSuppressed(copy(s, masker, depth + 1));
            }
        }
        return copy;
    }

    /** Stands in for an exception whose message held sensitive data; its message starts with the original type. */
    static final class MaskedException extends RuntimeException {
        MaskedException(String message, Throwable cause) {
            super(message, cause, true, true);
        }
    }
}
