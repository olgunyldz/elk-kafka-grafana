package io.github.olgunyldz.logmasking.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;

import java.util.Iterator;

/**
 * Logback appender that masks sensitive data and passes the event on to the appenders inside it.
 * <p>
 * Spring Boot apps don't need to configure it: {@link LogMasking#install} wraps every configured
 * appender at startup. With a plain {@code logback.xml}:
 * <pre>{@code
 * <appender name="MASKED_CONSOLE" class="io.github.olgunyldz.logmasking.logging.MaskingAppender">
 *   <disabledRules>phone</disabledRules>               <!-- optional, comma separated -->
 *   <extraSensitiveKeys>otp,customer_ref</extraSensitiveKeys> <!-- optional -->
 *   <appender-ref ref="CONSOLE"/>
 * </appender>
 * <root level="INFO"><appender-ref ref="MASKED_CONSOLE"/></root>
 * }</pre>
 */
public class MaskingAppender extends UnsynchronizedAppenderBase<ILoggingEvent>
        implements AppenderAttachable<ILoggingEvent> {

    private final AppenderAttachableImpl<ILoggingEvent> appenders = new AppenderAttachableImpl<>();
    private SensitiveDataMasker masker;
    private String disabledRules = "";
    private String extraSensitiveKeys = "";

    public void setDisabledRules(String disabledRules) {
        this.disabledRules = disabledRules;
    }

    public void setExtraSensitiveKeys(String extraSensitiveKeys) {
        this.extraSensitiveKeys = extraSensitiveKeys;
    }

    /** Uses this masker instead of building one from disabledRules / extraSensitiveKeys. */
    public void setMasker(SensitiveDataMasker masker) {
        this.masker = masker;
    }

    @Override
    public void start() {
        if (masker == null) {
            masker = SensitiveDataMasker.builder()
                    .disableRules(disabledRules.split(","))
                    .addSensitiveKeys(extraSensitiveKeys.split(","))
                    .build();
        }
        if (!appenders.iteratorForAppenders().hasNext()) {
            addWarn("MaskingAppender [" + getName() + "] has no appender-ref, nothing will be logged");
        }
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
        appenders.detachAndStopAllAppenders();
    }

    @Override
    protected void append(ILoggingEvent event) {
        appenders.appendLoopOnAppenders(new MaskedLoggingEvent(event, masker));
    }

    // ---- AppenderAttachable ----

    @Override
    public void addAppender(Appender<ILoggingEvent> appender) {
        appenders.addAppender(appender);
    }

    @Override
    public Iterator<Appender<ILoggingEvent>> iteratorForAppenders() {
        return appenders.iteratorForAppenders();
    }

    @Override
    public Appender<ILoggingEvent> getAppender(String name) {
        return appenders.getAppender(name);
    }

    @Override
    public boolean isAttached(Appender<ILoggingEvent> appender) {
        return appenders.isAttached(appender);
    }

    @Override
    public void detachAndStopAllAppenders() {
        appenders.detachAndStopAllAppenders();
    }

    @Override
    public boolean detachAppender(Appender<ILoggingEvent> appender) {
        return appenders.detachAppender(appender);
    }

    @Override
    public boolean detachAppender(String name) {
        return appenders.detachAppender(name);
    }
}
