package io.github.olgunyldz.logmasking.pci.kafka;

import io.github.olgunyldz.logmasking.pci.SensitiveDataDetector;
import io.github.olgunyldz.logmasking.pci.SensitiveDataEncryption;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.RecordInterceptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class PciKafkaAutoConfigurationTest {

    private static final String KEY = "pci.data-encryption-key=+WpfE866Ysy6Lp51Q61eBAcupVyUtI5nnyxwm6avujc=";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(PciKafkaAutoConfiguration.class));

    @Test
    void producerSideBeans() {
        runner.withBean(KafkaTemplate.class, () -> mock(KafkaTemplate.class))
                .withPropertyValues(KEY)
                .run(context -> {
                    assertThat(context).hasSingleBean(SensitiveDataEncryption.class);
                    assertThat(context).hasSingleBean(SensitiveDataDetector.class);
                    assertThat(context).hasSingleBean(PciKafkaTemplate.class);
                    assertThat(context).doesNotHaveBean(PciDecryptingRecordInterceptor.class);
                });
    }

    @Test
    void interceptorOnlyWhenConsumerGroupsAreConfigured() {
        runner.withPropertyValues(KEY, "pci.decrypt.consumer-groups=log-consumer,event-consumer")
                .run(context -> {
                    assertThat(context).hasSingleBean(PciDecryptingRecordInterceptor.class);
                    assertThat(context).doesNotHaveBean(PciKafkaTemplate.class);
                });
    }

    @Test
    void interceptorStepsAsideForTheAppsOwnRecordInterceptor() {
        // Spring Boot applies a RecordInterceptor only when there is exactly one
        RecordInterceptor<Object, Object> own = (record, consumer) -> record;
        runner.withBean(RecordInterceptor.class, () -> own)
                .withPropertyValues(KEY, "pci.decrypt.consumer-groups=log-consumer")
                .run(context -> {
                    assertThat(context).hasSingleBean(RecordInterceptor.class);
                    assertThat(context).doesNotHaveBean(PciDecryptingRecordInterceptor.class);
                });
    }

    @Test
    void failsFastWithoutAKey() {
        runner.withBean(KafkaTemplate.class, () -> mock(KafkaTemplate.class))
                .run(context -> assertThat(context).hasFailed()
                        .getFailure().rootCause().hasMessageContaining("pci.data-encryption-key"));
    }
}
