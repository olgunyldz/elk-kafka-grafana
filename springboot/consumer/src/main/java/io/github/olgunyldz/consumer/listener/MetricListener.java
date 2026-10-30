package io.github.olgunyldz.consumer.listener;

import io.github.olgunyldz.consumer.model.MetricMessage;
import io.github.olgunyldz.logmasking.logging.PciLogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class MetricListener {

    private static final Logger log = PciLogger.getLogger(MetricListener.class);

    @KafkaListener(topics = "${app.kafka.topics.metrics}", groupId = "metric-consumer", concurrency = "3")
    void onMetric(ConsumerRecord<String, MetricMessage> record, @Header(KafkaHeaders.GROUP_ID) String groupId) {
        var metric = record.value();
        log.info("[group {}] Key: {} | Value: {}={} {} | Partition: {} | Offset: {}",
                groupId, record.key(), metric.name(), metric.value(), metric.tags(),
                record.partition(), record.offset());
    }
}
