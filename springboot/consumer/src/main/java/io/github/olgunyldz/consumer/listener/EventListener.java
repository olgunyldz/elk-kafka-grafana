package io.github.olgunyldz.consumer.listener;

import io.github.olgunyldz.consumer.model.AppEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class EventListener {

    private static final Logger log = LoggerFactory.getLogger(EventListener.class);

    // A separate group: it tracks its own offsets, independent of log-consumer
    @KafkaListener(topics = "${app.kafka.topics.events}", groupId = "event-consumer", concurrency = "3")
    void onEvent(ConsumerRecord<String, AppEvent> record) {
        var event = record.value();
        log.info("[{}-{}@{}] {} {} {}",
                record.topic(), record.partition(), record.offset(),
                event.type(), event.aggregateId(), event.payload());
    }
}
