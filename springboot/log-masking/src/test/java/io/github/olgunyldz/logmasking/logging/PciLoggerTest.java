package io.github.olgunyldz.logmasking.logging;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/** PciLogger on its own: no MaskingAppender, the backend only ever receives masked text. */
class PciLoggerTest {

    private ListAppender<ILoggingEvent> captured;
    private Logger log;

    @BeforeEach
    void setUp() {
        LoggerContext context = new LoggerContext();
        captured = new ListAppender<>() {
            @Override
            protected void append(ILoggingEvent e) {
                e.getCallerData();     // Like a real appender: Logback computes caller data while logging
                super.append(e);
            }
        };
        captured.setContext(context);
        captured.start();
        ch.qos.logback.classic.Logger backend = context.getLogger("payments");
        backend.addAppender(captured);
        log = PciLogger.wrap(backend);
    }

    private ILoggingEvent last() {
        return captured.list.get(captured.list.size() - 1);
    }

    @Test
    void masksArgumentsBeforeTheBackendSeesThem() {
        log.info("payment with card {} for {}", "4111 1111 1111 1111", "john.doe@example.com");

        ILoggingEvent e = last();
        assertThat(e.getFormattedMessage()).isEqualTo("payment with card **** **** **** 1111 for j***@example.com");
        // The backend got the finished, masked text: no raw argument to rebuild it from
        assertThat(e.getMessage()).isEqualTo(e.getFormattedMessage());
        assertThat(e.getArgumentArray()).isNull();
    }

    @Test
    void masksAWholeJsonValue() {
        log.info("Received: {}", "{\"card\":\"4111111111111111\",\"cvv\":\"123\",\"iban\":\"TR330006100519786457841326\"}");

        assertThat(last().getFormattedMessage())
                .isEqualTo("Received: {\"card\":\"************1111\",\"cvv\":\"***\",\"iban\":\"TR********************1326\"}");
    }

    @Test
    void fluentApiAndKeyValues() {
        log.atWarn().setMessage("refund to {}").addArgument("GB82 WEST 1234 5698 7654 32").addKeyValue("tckn", "10000000146").log();

        assertThat(last().getFormattedMessage())
                .contains("refund to GB** **** **** **** **54 32")
                .contains("tckn=*********46");
    }

    @Test
    void masksExceptionMessagesAndCauses() {
        Exception e = new IllegalArgumentException("invalid card 5555555555554444",
                new IllegalStateException("password=hunter2"));
        log.error("payment failed", e);

        IThrowableProxy logged = last().getThrowableProxy();
        assertThat(logged.getMessage()).isEqualTo("java.lang.IllegalArgumentException: invalid card ************4444");
        assertThat(logged.getCause().getMessage()).isEqualTo("java.lang.IllegalStateException: password=***");
        // Same stack trace as the original
        assertThat(logged.getStackTraceElementProxyArray()[0].getStackTraceElement().getMethodName())
                .isEqualTo(e.getStackTrace()[0].getMethodName());
    }

    @Test
    void exceptionsWithoutSensitiveDataStayAsTheyAre() {
        Exception e = new IllegalStateException("broker not available");

        assertThat(MaskedThrowables.mask(e, SensitiveDataMasker.defaults())).isSameAs(e);
    }

    @Test
    void callerIsTheCallingCodeNotTheWrapper() {
        log.info("where am I");

        StackTraceElement caller = last().getCallerData()[0];
        assertThat(caller.getClassName()).isEqualTo(PciLoggerTest.class.getName());
        assertThat(caller.getMethodName()).isEqualTo("callerIsTheCallingCodeNotTheWrapper");
    }

    @Test
    void levelsFollowTheBackend() {
        log.debug("debug {}", "4111111111111111");   // Logback's default level is DEBUG

        assertThat(last().getFormattedMessage()).isEqualTo("debug ************1111");
        assertThat(log.isInfoEnabled()).isTrue();
        assertThat(log.getName()).isEqualTo("payments");
    }
}
