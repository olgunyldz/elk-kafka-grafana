package io.github.olgunyldz.logmasking.pci.kafka;

import io.github.olgunyldz.logmasking.pci.SensitiveDataDetector;
import io.github.olgunyldz.logmasking.pci.SensitiveDataEncryption;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.RecordInterceptor;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

/**
 * Registers the PCI beans in any Spring Boot app that has this library on the classpath.
 * Listed in META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports.
 *
 * <pre>
 * pci:
 *   data-encryption-key: base64 32-byte key          # required
 *   decrypt:
 *     consumer-groups: group-a,group-b               # optional: groups that see plaintext, * = all
 * </pre>
 */
@AutoConfiguration(afterName = "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration")
@ConditionalOnClass(KafkaTemplate.class)
@EnableConfigurationProperties(PciKafkaAutoConfiguration.PciProperties.class)
public class PciKafkaAutoConfiguration {

    @ConfigurationProperties("pci")
    public record PciProperties(String dataEncryptionKey, Decrypt decrypt) {
        public record Decrypt(List<String> consumerGroups) {
        }

        List<String> decryptGroups() {
            return decrypt == null || decrypt.consumerGroups() == null ? List.of() : decrypt.consumerGroups();
        }
    }

    @Bean
    @ConditionalOnMissingBean
    SensitiveDataEncryption sensitiveDataEncryption(PciProperties properties) {
        return new SensitiveDataEncryption(properties.dataEncryptionKey());
    }

    @Bean
    @ConditionalOnMissingBean
    SensitiveDataDetector sensitiveDataDetector(SensitiveDataEncryption encryption) {
        return new SensitiveDataDetector(encryption);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(KafkaTemplate.class)
    PciKafkaTemplate pciKafkaTemplate(KafkaTemplate<String, Object> kafkaTemplate,
                                      SensitiveDataDetector detector,
                                      ObjectProvider<JsonMapper> jsonMapper) {
        return new PciKafkaTemplate(kafkaTemplate, detector, jsonMapper.getIfAvailable(JsonMapper::new));
    }

    /**
     * Spring Boot applies a RecordInterceptor bean to every @KafkaListener, but only when there is
     * exactly one. So this one steps aside if the app defines any RecordInterceptor of its own.
     */
    @Bean
    @ConditionalOnMissingBean(RecordInterceptor.class)
    @ConditionalOnProperty(prefix = "pci.decrypt", name = "consumer-groups")
    PciDecryptingRecordInterceptor pciDecryptingRecordInterceptor(SensitiveDataEncryption encryption,
                                                                  ObjectProvider<JsonMapper> jsonMapper,
                                                                  PciProperties properties) {
        return new PciDecryptingRecordInterceptor(encryption, jsonMapper.getIfAvailable(JsonMapper::new),
                properties.decryptGroups());
    }
}
