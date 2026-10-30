package io.github.olgunyldz.consumer.config;

import io.github.olgunyldz.logmasking.logging.PciLogger;
import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorConfig {

    private static final Logger log = PciLogger.getLogger(KafkaErrorConfig.class);

    /**
     * Retries a failing record 3 times, 1 second apart, then logs it and moves on,
     * so one bad record can't block its partition forever. Records that can't be
     * deserialized at all are not retried. Spring Boot applies this bean to every listener.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler(
                (record, ex) -> log.error("Skipping {}-{}@{} after retries: {}",
                        record.topic(), record.partition(), record.offset(), ex.getMessage()),
                new FixedBackOff(1000L, 3));
    }
}
