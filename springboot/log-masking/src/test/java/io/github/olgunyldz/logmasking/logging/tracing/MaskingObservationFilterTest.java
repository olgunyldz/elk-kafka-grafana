package io.github.olgunyldz.logmasking.logging.tracing;

import io.micrometer.common.KeyValue;
import io.micrometer.observation.Observation;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class MaskingObservationFilterTest {

    private final MaskingObservationFilter filter = new MaskingObservationFilter();

    @Test
    void masksSensitiveDataInKeyValues() {
        var context = new Observation.Context();
        context.addHighCardinalityKeyValue(KeyValue.of("http.url", "/api/pay?card=4111111111111111&email=jane.doe@example.com"));
        context.addLowCardinalityKeyValue(KeyValue.of("uri", "/api/pay"));

        filter.map(context);

        assertThat(context.getHighCardinalityKeyValue("http.url").getValue())
                .isEqualTo("/api/pay?card=************1111&email=j***@example.com");
        assertThat(context.getLowCardinalityKeyValue("uri").getValue()).isEqualTo("/api/pay");
    }

    @Test
    void leavesOrdinaryKeyValuesAlone() {
        var context = new Observation.Context();
        context.addLowCardinalityKeyValue(KeyValue.of("messaging.destination.name", "app-events"));
        context.addHighCardinalityKeyValue(KeyValue.of("messaging.kafka.message.offset", "1234567890123"));

        filter.map(context);

        assertThat(context.getLowCardinalityKeyValue("messaging.destination.name").getValue()).isEqualTo("app-events");
        assertThat(context.getHighCardinalityKeyValue("messaging.kafka.message.offset").getValue()).isEqualTo("1234567890123");
    }

    @Test
    void autoConfigurationRegistersTheFilterUnlessMaskingIsOff() {
        var runner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ObservationMaskingAutoConfiguration.class));

        runner.run(context -> assertThat(context).hasSingleBean(MaskingObservationFilter.class));
        runner.withPropertyValues("logging.masking.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(MaskingObservationFilter.class));
    }
}
