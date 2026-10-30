package io.github.olgunyldz.logmasking.logging.spring;

import ch.qos.logback.classic.LoggerContext;
import io.github.olgunyldz.logmasking.logging.LogMasking;
import io.github.olgunyldz.logmasking.logging.PciLogger;
import io.github.olgunyldz.logmasking.logging.SensitiveDataMasker;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.boot.context.logging.LoggingApplicationListener;
import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * Turns log masking on in a Spring Boot app: right after Spring Boot has configured logging
 * (plain text or structured / ECS JSON), every Logback appender is wrapped by a masking appender.
 * Registered in META-INF/spring.factories, so adding the jar is enough.
 * <p>
 * Properties:
 * <ul>
 *   <li>{@code logging.masking.enabled} (default true)</li>
 *   <li>{@code logging.masking.disabled-rules}: comma separated, e.g. {@code phone,email}</li>
 *   <li>{@code logging.masking.extra-sensitive-keys}: comma separated keys whose values are masked, e.g. {@code otp,customer_ref}</li>
 *   <li>{@code logging.masking.extra-personal-keys}: comma separated personal keys, whose values may contain spaces,
 *       e.g. {@code mother_maiden_name}</li>
 * </ul>
 */
public class LogMaskingApplicationListener implements ApplicationListener<ApplicationEnvironmentPreparedEvent>, Ordered {

    @Override
    public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
        ConfigurableEnvironment env = event.getEnvironment();
        if (!env.getProperty("logging.masking.enabled", Boolean.class, true)) {
            return;
        }
        if (!(LoggerFactory.getILoggerFactory() instanceof LoggerContext context)) {
            return;     // Not Logback
        }
        SensitiveDataMasker masker = SensitiveDataMasker.builder()
                .disableRules(env.getProperty("logging.masking.disabled-rules", "").split(","))
                .addSensitiveKeys(env.getProperty("logging.masking.extra-sensitive-keys", "").split(","))
                .addPersonalKeys(env.getProperty("logging.masking.extra-personal-keys", "").split(","))
                .build();
        // The same rules for PciLogger (the app's own log lines) and for the appenders (everything else)
        PciLogger.configure(masker);
        int wrapped = LogMasking.install(context, masker);
        LoggerFactory.getLogger(LogMaskingApplicationListener.class)
                .info("Log masking enabled: PciLogger and {} appender(s), rules: {}", wrapped, masker.ruleNames());
    }

    /** After {@link LoggingApplicationListener}, which configures Logback on the same event. */
    @Override
    public int getOrder() {
        return LoggingApplicationListener.DEFAULT_ORDER + 1;
    }
}
