package io.github.olgunyldz.logmasking.logging;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MaskingAppenderTest {

    private LoggerContext context;
    private Logger logger;
    private ListAppender<ILoggingEvent> captured;

    @BeforeEach
    void setUp() {
        context = new LoggerContext();
        context.setMDCAdapter(new LogbackMDCAdapter());
        logger = context.getLogger("test");
        captured = new ListAppender<>();
        captured.setContext(context);
        captured.setName("LIST");
        captured.start();
        logger.addAppender(captured);
        // What the Spring Boot listener does: wrap the configured appenders
        assertThat(LogMasking.install(context, SensitiveDataMasker.defaults())).isEqualTo(1);
    }

    @Test
    void masksFormattedMessageAndHidesArguments() {
        logger.info("payment with card {} for {}", "4111111111111111", "john.doe@example.com");

        ILoggingEvent event = captured.list.get(0);
        assertThat(event.getFormattedMessage()).isEqualTo("payment with card ************1111 for j***@example.com");
        assertThat(event.getMessage()).isEqualTo(event.getFormattedMessage());
        assertThat(event.getArgumentArray()).isNull();
    }

    @Test
    void masksExceptionMessagesAndCauses() {
        Exception cause = new IllegalStateException("token=abc123");
        logger.error("failed", new IllegalArgumentException("invalid card 5555555555554444", cause));

        var thrown = captured.list.get(0).getThrowableProxy();
        assertThat(thrown.getMessage()).isEqualTo("invalid card ************4444");
        assertThat(thrown.getCause().getMessage()).isEqualTo("token=***");
    }

    @Test
    void masksMdcValues() {
        // The test's own LoggerContext reads its own MDC adapter, not the global MDC
        context.getMDCAdapter().put("customerEmail", "jane@example.com");
        try {
            logger.info("hello");
            // Logback reads the MDC lazily; a real appender formats the event right here
            assertThat(captured.list.get(0).getMDCPropertyMap()).containsEntry("customerEmail", "j***@example.com");
        } finally {
            context.getMDCAdapter().clear();
        }
    }

    @Test
    void leavesTraceIdsInTheMdcAlone() {
        // A trace id that contains a valid national ID digit run: masking it would break log-to-trace links
        String traceId = "abc10000000146def0123456789abcde";
        context.getMDCAdapter().put("traceId", traceId);
        context.getMDCAdapter().put("note", "tckn 10000000146");
        try {
            logger.info("hello");
            assertThat(captured.list.get(0).getMDCPropertyMap())
                    .containsEntry("traceId", traceId)
                    .containsEntry("note", "tckn *********46");
        } finally {
            context.getMDCAdapter().clear();
        }
    }

    @Test
    void masksKeyValuePairs() {
        logger.atInfo().setMessage("refund").addKeyValue("iban", "TR330006100519786457841326").log();

        assertThat(captured.list.get(0).getKeyValuePairs())
                .singleElement()
                .satisfies(p -> assertThat(p.value).isEqualTo("TR********************1326"));
    }

    @Test
    void installTwiceDoesNotWrapTwice() {
        assertThat(LogMasking.install(context, SensitiveDataMasker.defaults())).isZero();

        logger.info("pin=1234");

        assertThat(captured.list).hasSize(1);
        assertThat(captured.list.get(0).getFormattedMessage()).endsWith("pin=***");
    }
}
