package io.github.olgunyldz.logmasking.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import org.slf4j.Marker;
import org.slf4j.event.KeyValuePair;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A read-only view of a logging event with sensitive data masked: the formatted message,
 * MDC values, key-value pairs and exception messages (including causes).
 * <p>
 * {@link #getMessage()} returns the already formatted, masked message and {@link #getArgumentArray()}
 * returns null, so an encoder (plain pattern or ECS JSON) can't rebuild the unmasked text from the
 * original arguments.
 */
final class MaskedLoggingEvent implements ILoggingEvent {

    /**
     * MDC values that are random ids, never data: tracing's trace and span ids. A 32-digit hex trace id
     * sometimes contains a digit run that a number rule would mask, which would break the link from a
     * log line to its trace.
     */
    static final Set<String> UNMASKED_MDC_KEYS = Set.of("traceId", "spanId");

    private final ILoggingEvent delegate;
    private final SensitiveDataMasker masker;
    private String maskedMessage;
    private Map<String, String> maskedMdc;
    private List<KeyValuePair> maskedKeyValuePairs;
    private IThrowableProxy maskedThrowable;

    MaskedLoggingEvent(ILoggingEvent delegate, SensitiveDataMasker masker) {
        this.delegate = delegate;
        this.masker = masker;
    }

    @Override
    public String getFormattedMessage() {
        if (maskedMessage == null) {
            maskedMessage = masker.mask(delegate.getFormattedMessage());
        }
        return maskedMessage;
    }

    @Override
    public String getMessage() {
        return getFormattedMessage();
    }

    @Override
    public Object[] getArgumentArray() {
        return null;
    }

    @Override
    public Map<String, String> getMDCPropertyMap() {
        if (maskedMdc == null) {
            Map<String, String> mdc = delegate.getMDCPropertyMap();
            if (mdc == null || mdc.isEmpty()) {
                maskedMdc = mdc;
            } else {
                Map<String, String> copy = new LinkedHashMap<>();
                mdc.forEach((k, v) -> copy.put(k, UNMASKED_MDC_KEYS.contains(k) ? v : masker.mask(v)));
                maskedMdc = copy;
            }
        }
        return maskedMdc;
    }

    @Override
    @SuppressWarnings("deprecation")
    public Map<String, String> getMdc() {
        return getMDCPropertyMap();
    }

    @Override
    public List<KeyValuePair> getKeyValuePairs() {
        if (maskedKeyValuePairs == null) {
            List<KeyValuePair> pairs = delegate.getKeyValuePairs();
            maskedKeyValuePairs = pairs == null ? null : pairs.stream()
                    .map(p -> p.value instanceof String s ? new KeyValuePair(p.key, masker.mask(s)) : p)
                    .toList();
        }
        return maskedKeyValuePairs;
    }

    @Override
    public IThrowableProxy getThrowableProxy() {
        if (maskedThrowable == null && delegate.getThrowableProxy() != null) {
            maskedThrowable = MaskedThrowableProxy.of(delegate.getThrowableProxy(), masker);
        }
        return maskedThrowable;
    }

    // ---- Unchanged ----

    @Override
    public String getThreadName() {
        return delegate.getThreadName();
    }

    @Override
    public Level getLevel() {
        return delegate.getLevel();
    }

    @Override
    public String getLoggerName() {
        return delegate.getLoggerName();
    }

    @Override
    public LoggerContextVO getLoggerContextVO() {
        return delegate.getLoggerContextVO();
    }

    @Override
    public StackTraceElement[] getCallerData() {
        return delegate.getCallerData();
    }

    @Override
    public boolean hasCallerData() {
        return delegate.hasCallerData();
    }

    @Override
    public List<Marker> getMarkerList() {
        return delegate.getMarkerList();
    }

    @Override
    public long getTimeStamp() {
        return delegate.getTimeStamp();
    }

    @Override
    public int getNanoseconds() {
        return delegate.getNanoseconds();
    }

    @Override
    public Instant getInstant() {
        return delegate.getInstant();
    }

    @Override
    public long getSequenceNumber() {
        return delegate.getSequenceNumber();
    }

    @Override
    public void prepareForDeferredProcessing() {
        delegate.prepareForDeferredProcessing();
        getFormattedMessage();
        getMDCPropertyMap();
    }
}
