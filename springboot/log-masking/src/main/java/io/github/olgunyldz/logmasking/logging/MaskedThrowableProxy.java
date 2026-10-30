package io.github.olgunyldz.logmasking.logging;

import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;

/**
 * An exception as Logback sees it, with the message masked, e.g.
 * {@code IllegalArgumentException: invalid card 4111111111111111}. Causes and suppressed exceptions too.
 * Stack frames are kept as they are: they hold class and method names, not data.
 */
final class MaskedThrowableProxy implements IThrowableProxy {

    private static final int MAX_DEPTH = 50;

    private final IThrowableProxy delegate;
    private final SensitiveDataMasker masker;
    private final IThrowableProxy cause;
    private final IThrowableProxy[] suppressed;

    private MaskedThrowableProxy(IThrowableProxy delegate, SensitiveDataMasker masker, int depth) {
        this.delegate = delegate;
        this.masker = masker;
        // Depth guard: a cyclic cause chain must not recurse forever
        this.cause = delegate.getCause() == null || depth > MAX_DEPTH ? null
                : new MaskedThrowableProxy(delegate.getCause(), masker, depth + 1);
        IThrowableProxy[] s = delegate.getSuppressed();
        if (s == null || depth > MAX_DEPTH) {
            this.suppressed = s;
        } else {
            this.suppressed = new IThrowableProxy[s.length];
            for (int i = 0; i < s.length; i++) {
                this.suppressed[i] = new MaskedThrowableProxy(s[i], masker, depth + 1);
            }
        }
    }

    static IThrowableProxy of(IThrowableProxy proxy, SensitiveDataMasker masker) {
        return proxy == null ? null : new MaskedThrowableProxy(proxy, masker, 0);
    }

    @Override
    public String getMessage() {
        return masker.mask(delegate.getMessage());
    }

    @Override
    public String getOverridingMessage() {
        return masker.mask(delegate.getOverridingMessage());
    }

    @Override
    public IThrowableProxy getCause() {
        return cause;
    }

    @Override
    public IThrowableProxy[] getSuppressed() {
        return suppressed;
    }

    @Override
    public String getClassName() {
        return delegate.getClassName();
    }

    @Override
    public StackTraceElementProxy[] getStackTraceElementProxyArray() {
        return delegate.getStackTraceElementProxyArray();
    }

    @Override
    public int getCommonFrames() {
        return delegate.getCommonFrames();
    }

    @Override
    public boolean isCyclic() {
        return delegate.isCyclic();
    }
}
