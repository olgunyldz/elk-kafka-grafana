package io.github.olgunyldz.consumer.listener;

import io.github.olgunyldz.consumer.model.MetricMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MetricListener {

    private static final Logger log = LoggerFactory.getLogger(MetricListener.class);

    @KafkaListener(topics = "${app.kafka.topics.metrics}", groupId = "metric-consumer", concurrency = "3")
    void onMetric(ConsumerRecord<String, MetricMessage> record) {
        var metric = record.value();
        log.info("[{}-{}@{}] {} {}={} {}",
                record.topic(), record.partition(), record.offset(),
                metric.service(), metric.name(), metric.value(), metric.tags());
    }
}
