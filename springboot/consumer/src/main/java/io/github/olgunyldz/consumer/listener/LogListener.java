package io.github.olgunyldz.consumer.listener;

import io.github.olgunyldz.consumer.client.MessageApiClient;
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

    private final MessageApiClient messageApi;

    public LogListener(MessageApiClient messageApi) {
        this.messageApi = messageApi;
    }

    // 3 threads, one per partition of the log messages topic. The id names the listener in metrics
    // (log-listener-0..2); groupId takes precedence over it for the consumer group
    @KafkaListener(id = "log-listener", topics = "${app.kafka.topics.logs}", groupId = "log-consumer", concurrency = "3")
    void onLog(ConsumerRecord<String, LogMessage> record, @Header(KafkaHeaders.GROUP_ID) String groupId) {
        var msg = record.value();
        log.info("[group {}] Key: {} | Value: {} {} {} | Partition: {} | Offset: {}",
                groupId, record.key(), msg.level(), msg.service(), msg.message(),
                record.partition(), record.offset());
        // Tell message-api (REST): the trace continues there
        messageApi.notify(record, msg.level(), groupId);
    }
}
