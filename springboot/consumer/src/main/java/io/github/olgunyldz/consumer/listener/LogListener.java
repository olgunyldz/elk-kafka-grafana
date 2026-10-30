package io.github.olgunyldz.consumer.listener;

import io.github.olgunyldz.consumer.model.LogMessage;
import io.github.olgunyldz.logmasking.logging.PciLogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class LogListener {

    // The record arrives decrypted for this group: PciLogger masks it again before it's written
    private static final Logger log = PciLogger.getLogger(LogListener.class);

    // 3 threads, one per partition of the log messages topic
    @KafkaListener(topics = "${app.kafka.topics.logs}", groupId = "log-consumer", concurrency = "3")
    void onLog(ConsumerRecord<String, LogMessage> record, @Header(KafkaHeaders.GROUP_ID) String groupId) {
        var msg = record.value();
        log.info("[group {}] Key: {} | Value: {} {} {} | Partition: {} | Offset: {}",
                groupId, record.key(), msg.level(), msg.service(), msg.message(),
                record.partition(), record.offset());
    }
}
