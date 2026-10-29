package io.github.olgunyldz.consumer.listener;

import io.github.olgunyldz.consumer.model.LogMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class LogListener {

    private static final Logger log = LoggerFactory.getLogger(LogListener.class);

    // 3 threads, one per partition of app-logs
    @KafkaListener(topics = "${app.kafka.topics.logs}", groupId = "log-consumer", concurrency = "3")
    void onLog(ConsumerRecord<String, LogMessage> record) {
        var msg = record.value();
        log.info("[{}-{}@{}] {} {} {}: {}",
                record.topic(), record.partition(), record.offset(),
                msg.timestamp(), msg.level(), msg.service(), msg.message());
    }
}
