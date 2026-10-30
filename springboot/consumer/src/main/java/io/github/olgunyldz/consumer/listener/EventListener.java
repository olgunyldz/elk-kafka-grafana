package io.github.olgunyldz.consumer.listener;

import io.github.olgunyldz.consumer.client.MessageApiClient;
import io.github.olgunyldz.consumer.model.AppEvent;
import io.github.olgunyldz.logmasking.logging.PciLogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class EventListener {

    private static final Logger log = PciLogger.getLogger(EventListener.class);

    private final MessageApiClient messageApi;

    public EventListener(MessageApiClient messageApi) {
        this.messageApi = messageApi;
    }

    // A separate group: it tracks its own offsets, independent of log-consumer
    @KafkaListener(id = "event-listener", topics = "${app.kafka.topics.events}", groupId = "event-consumer", concurrency = "3")
    void onEvent(ConsumerRecord<String, AppEvent> record, @Header(KafkaHeaders.GROUP_ID) String groupId) {
        var event = record.value();
        log.info("[group {}] Key: {} | Value: {} {} | Partition: {} | Offset: {}",
                groupId, record.key(), event.type(), event.payload(), record.partition(), record.offset());
        // Tell message-api (REST): the trace continues there
        messageApi.notify(record, event.type(), groupId);
    }
}
