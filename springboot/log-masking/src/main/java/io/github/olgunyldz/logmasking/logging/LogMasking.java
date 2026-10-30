package io.github.olgunyldz.logmasking.logging;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Puts a {@link MaskingAppender} in front of every appender of an already configured Logback context. */
public final class LogMasking {

    private LogMasking() {
    }

    /**
     * Wraps every appender attached to any logger, so all events are masked before they are written.
     * Safe to call more than once: appenders that are already masked are left alone.
     *
     * @return how many appenders were wrapped
     */
    public static int install(LoggerContext context, SensitiveDataMasker masker) {
        Map<Appender<ILoggingEvent>, MaskingAppender> wrapped = new IdentityHashMap<>();
        for (Logger logger : context.getLoggerList()) {
            List<Appender<ILoggingEvent>> attached = new ArrayList<>();
            for (Iterator<Appender<ILoggingEvent>> it = logger.iteratorForAppenders(); it.hasNext(); ) {
                attached.add(it.next());
            }
            for (Appender<ILoggingEvent> appender : attached) {
                if (appender instanceof MaskingAppender) {
                    continue;
                }
                // An appender shared by several loggers gets one MaskingAppender
                MaskingAppender masking = wrapped.computeIfAbsent(appender, a -> {
                    MaskingAppender m = new MaskingAppender();
                    m.setContext(context);
                    m.setName("MASKED_" + a.getName());
                    m.setMasker(masker);
                    m.addAppender(a);
                    m.start();
                    return m;
                });
                logger.detachAppender(appender);
                logger.addAppender(masking);
            }
        }
        return wrapped.size();
    }
}
