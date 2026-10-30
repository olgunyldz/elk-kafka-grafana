package io.github.olgunyldz.logmasking.audit;

import io.github.olgunyldz.logmasking.pci.SensitiveDataDetector;
import io.github.olgunyldz.logmasking.pci.SensitiveDataEncryption;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.net.InetAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Audit logging for a Spring Boot app, turned on with {@code audit.enabled=true}:
 * <pre>
 * audit:
 *   enabled: true
 *   hmac-key: base64, at least 32 bytes   # its own secret, not the data encryption key
 *   key-id: audit-v1                       # written into every record, for key rotation
 *   topic: audit-events
 *   heartbeat-interval: 60s
 * </pre>
 * Registers an {@link AuditLogger} with its own Kafka producer (acks=all, idempotent, short timeouts so a
 * fail-closed answer comes quickly) and the {@link AuditLifecycle} (chain start, heartbeats, chain end).
 */
@AutoConfiguration(afterName = "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration")
@ConditionalOnClass(KafkaTemplate.class)
@ConditionalOnBooleanProperty("audit.enabled")
@EnableConfigurationProperties(AuditAutoConfiguration.AuditProperties.class)
public class AuditAutoConfiguration {

    @ConfigurationProperties("audit")
    public record AuditProperties(boolean enabled, String hmacKey, String keyId, String topic,
                                  Duration heartbeatInterval, Duration sendTimeout) {
        public AuditProperties {
            keyId = keyId == null ? "audit-v1" : keyId;
            topic = topic == null ? "audit-events" : topic;
            heartbeatInterval = heartbeatInterval == null ? Duration.ofSeconds(60) : heartbeatInterval;
            sendTimeout = sendTimeout == null ? Duration.ofSeconds(15) : sendTimeout;
        }
    }

    @Bean
    @ConditionalOnMissingBean
    AuditLogger auditLogger(AuditProperties properties, Environment env,
                            ObjectProvider<SensitiveDataDetector> detector,
                            ObjectProvider<AuditMetrics> metrics) {
        var producerProps = new HashMap<String, Object>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("spring.kafka.bootstrap-servers", "localhost:9092"));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        String service = env.getProperty("spring.application.name", "app");
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, service + "-audit");
        var template = new KafkaTemplate<String, String>(new DefaultKafkaProducerFactory<>(Map.copyOf(producerProps)));

        // The detector needs the PCI encryption bean; an app without it still gets the checks
        SensitiveDataDetector checks = detector.getIfAvailable(
                () -> new SensitiveDataDetector(new SensitiveDataEncryption(SensitiveDataEncryption.newBase64Key())));
        BiConsumer<String, String> listener = metrics.getIfAvailable(() -> new AuditMetrics(null));
        return new AuditLogger(template, new AuditChain(properties.hmacKey(), properties.keyId()), checks,
                properties.topic(), service, hostName(), properties.sendTimeout(), listener);
    }

    @Bean
    @ConditionalOnMissingBean
    AuditLifecycle auditLifecycle(AuditLogger audit, AuditProperties properties, Environment env) {
        return new AuditLifecycle(audit, env.getProperty("spring.application.name", "app"), properties.heartbeatInterval());
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "io.micrometer.core.instrument.MeterRegistry")
    static class MetricsConfiguration {
        @Bean
        @ConditionalOnMissingBean
        AuditMetrics auditMetrics(ObjectProvider<MeterRegistry> registry) {
            return new AuditMetrics(registry.getIfAvailable());
        }
    }

    /** audit_records_total{action, result="written"|"failed"} */
    public static class AuditMetrics implements BiConsumer<String, String> {
        private final MeterRegistry registry;

        AuditMetrics(MeterRegistry registry) {
            this.registry = registry;
        }

        @Override
        public void accept(String action, String result) {
            if (registry != null) {
                Counter.builder("audit.records").description("Audit records written or failed")
                        .tag("action", action).tag("result", result).register(registry).increment();
            }
        }
    }

    private static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
