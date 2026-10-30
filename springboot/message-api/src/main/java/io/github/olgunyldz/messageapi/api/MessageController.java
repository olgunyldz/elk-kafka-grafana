package io.github.olgunyldz.messageapi.api;

import io.github.olgunyldz.logmasking.logging.PciLogger;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.PositiveOrZero;
import org.slf4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

/**
 * Receives a notice from the consumer for every record it handled, logs it and acknowledges it.
 * The notice describes the record (where it was in Kafka, its key and type), not its content:
 * the consumer holds decrypted card and personal data, and that must not travel further.
 */
@RestController
@RequestMapping("/api")
public class MessageController {

    private static final Logger log = PciLogger.getLogger(MessageController.class);

    /**
     * @param handledBy which consumer instance handled the record (its host name: one per replica)
     */
    public record MessageNotice(
            @NotBlank String topic,
            @PositiveOrZero int partition,
            @PositiveOrZero long offset,
            String key,
            @NotBlank String type,
            @NotBlank String consumerGroup,
            @NotBlank String handledBy) {
    }

    public record Ack(String ackId, Instant receivedAt, String status) {
    }

    @PostMapping("/messages")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Ack receive(@Valid @RequestBody MessageNotice notice) {
        log.info("Message received from {} ({}): {} partition {} offset {} key={} type={}",
                notice.handledBy(), notice.consumerGroup(), notice.topic(), notice.partition(), notice.offset(),
                notice.key(), notice.type());
        return new Ack(UUID.randomUUID().toString(), Instant.now(), "RECEIVED");
    }
}
