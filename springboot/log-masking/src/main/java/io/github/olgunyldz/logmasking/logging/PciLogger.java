package io.github.olgunyldz.logmasking.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.helpers.AbstractLogger;
import org.slf4j.helpers.MessageFormatter;
import org.slf4j.spi.LocationAwareLogger;

import java.util.Objects;

/**
 * An SLF4J {@link Logger} that masks card and personal data itself, before anything reaches the logging
 * backend. Use it instead of {@code LoggerFactory.getLogger}:
 * <pre>{@code
 * private static final Logger log = PciLogger.getLogger(PaymentService.class);
 *
 * log.info("payment with card {} for {}", card, email);   // -> "payment with card ************1111 for j***@example.com"
 * }</pre>
 * <ul>
 *   <li>the message is formatted first, then masked, so placeholders, markers and the fluent API
 *       ({@code log.atInfo().addKeyValue(...)}) work as usual and every argument is covered</li>
 *   <li>exception messages and their causes are masked too</li>
 *   <li>it doesn't depend on Spring or Logback: any SLF4J backend gets the masked text</li>
 *   <li>caller data (class and line in the log pattern) still points at the calling code</li>
 * </ul>
 * The rules come from {@link #configure(SensitiveDataMasker)}; the Spring Boot listener sets them from the
 * {@code logging.masking.*} properties. Default: {@link SensitiveDataMasker#defaults()}.
 * <p>
 * Logs written by other libraries (Kafka client, Spring, ...) don't go through this class; the
 * {@link MaskingAppender}, installed automatically in Spring Boot apps, covers them.
 */
public final class PciLogger extends AbstractLogger {

    /** Frames of this class and of AbstractLogger are skipped when Logback looks for the caller. */
    private static final String FQCN = AbstractLogger.class.getName();

    private static volatile SensitiveDataMasker masker = SensitiveDataMasker.defaults();

    private final Logger delegate;

    private PciLogger(Logger delegate) {
        this.delegate = Objects.requireNonNull(delegate);
        this.name = delegate.getName();
    }

    public static Logger getLogger(Class<?> type) {
        return new PciLogger(LoggerFactory.getLogger(type));
    }

    public static Logger getLogger(String name) {
        return new PciLogger(LoggerFactory.getLogger(name));
    }

    /** Wraps an existing logger. */
    public static Logger wrap(Logger logger) {
        return logger instanceof PciLogger ? logger : new PciLogger(logger);
    }

    /** Rules used by every PciLogger from now on. */
    public static void configure(SensitiveDataMasker newMasker) {
        masker = Objects.requireNonNull(newMasker);
    }

    public static SensitiveDataMasker masker() {
        return masker;
    }

    @Override
    protected String getFullyQualifiedCallerName() {
        return FQCN;
    }

    @Override
    protected void handleNormalizedLoggingCall(Level level, Marker marker, String pattern, Object[] args, Throwable t) {
        SensitiveDataMasker m = masker;
        // Format first: the arguments are where the sensitive data usually is
        String message = m.mask(MessageFormatter.basicArrayFormat(pattern, args));
        Throwable throwable = MaskedThrowables.mask(t, m);
        if (delegate instanceof LocationAwareLogger located) {
            // No arguments: a masked message containing "{}" is printed as it is
            located.log(marker, FQCN, level.toInt(), message, null, throwable);
            return;
        }
        switch (level) {
            case TRACE -> delegate.trace(marker, message, throwable);
            case DEBUG -> delegate.debug(marker, message, throwable);
            case INFO -> delegate.info(marker, message, throwable);
            case WARN -> delegate.warn(marker, message, throwable);
            case ERROR -> delegate.error(marker, message, throwable);
        }
    }

    // ---- Level checks: answered by the real logger ----

    @Override
    public boolean isTraceEnabled() {
        return delegate.isTraceEnabled();
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return delegate.isTraceEnabled(marker);
    }

    @Override
    public boolean isDebugEnabled() {
        return delegate.isDebugEnabled();
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return delegate.isDebugEnabled(marker);
    }

    @Override
    public boolean isInfoEnabled() {
        return delegate.isInfoEnabled();
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return delegate.isInfoEnabled(marker);
    }

    @Override
    public boolean isWarnEnabled() {
        return delegate.isWarnEnabled();
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return delegate.isWarnEnabled(marker);
    }

    @Override
    public boolean isErrorEnabled() {
        return delegate.isErrorEnabled();
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return delegate.isErrorEnabled(marker);
    }
}
