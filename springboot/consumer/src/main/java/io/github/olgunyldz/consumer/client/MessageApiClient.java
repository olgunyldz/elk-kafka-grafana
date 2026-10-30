package io.github.olgunyldz.consumer.client;

import io.github.olgunyldz.logmasking.logging.PciLogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Tells message-api about every record a listener handled. Built from Spring Boot's RestClient.Builder,
 * so each call gets a client span and the traceparent header: the trace continues into message-api.
 * <p>
 * Only the record's coordinates and type are sent, never its value: the listeners see decrypted card
 * and personal data, and that must not leave this service.
 */
@Component
public class MessageApiClient {

    private static final Logger log = PciLogger.getLogger(MessageApiClient.class);

    /** Mirrors message-api's MessageNotice. handledBy = this replica's host name. */
    public record MessageNotice(String topic, int partition, long offset, String key, String type,
                                String consumerGroup, String handledBy) {
    }

    public record Ack(String ackId, String receivedAt, String status) {
    }

    private final RestClient restClient;
    private final String instance;

    public MessageApiClient(RestClient.Builder builder, @Value("${app.message-api.url}") String baseUrl) {
        // Timeouts come from application.yml: a slow message-api must not stall the listener threads
        this.restClient = builder.baseUrl(baseUrl).build();
        this.instance = hostName();
    }

    /**
     * Sends the notice. A failure is logged and swallowed: the record was already handled, and
     * message-api being down must not make Kafka redeliver it. The failed call still shows as an
     * error span in the trace.
     */
    public void notify(ConsumerRecord<String, ?> record, String type, String consumerGroup) {
        var notice = new MessageNotice(record.topic(), record.partition(), record.offset(), record.key(),
                type, consumerGroup, instance);
        try {
            Ack ack = restClient.post().uri("/api/messages").body(notice).retrieve().body(Ack.class);
            log.debug("message-api acknowledged {} partition {} offset {}: {}", record.topic(), record.partition(), record.offset(),
                    ack == null ? "-" : ack.ackId());
        } catch (RestClientException e) {
            log.warn("message-api call failed for {} partition {} offset {}: {}", record.topic(), record.partition(), record.offset(),
                    e.getMessage());
        }
    }

    public String instance() {
        return instance;
    }

    private static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }
}
