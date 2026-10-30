package io.github.olgunyldz.producer.api;

import io.github.olgunyldz.logmasking.pci.kafka.PciKafkaTemplate.PciSendResult;
import io.github.olgunyldz.producer.model.AppEvent;
import io.github.olgunyldz.producer.model.LogMessage;
import io.github.olgunyldz.producer.model.MetricMessage;
import io.github.olgunyldz.producer.service.KafkaPublisher;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api")
public class PublishController {

    private final KafkaPublisher publisher;

    public PublishController(KafkaPublisher publisher) {
        this.publisher = publisher;
    }

    public record LogRequest(
            @NotBlank String service,
            @NotBlank @Pattern(regexp = "TRACE|DEBUG|INFO|WARN|ERROR") String level,
            @NotBlank String message) {
    }

    public record EventRequest(
            @NotBlank String type,
            @NotBlank String aggregateId,
            Map<String, Object> payload) {
    }

    public record MetricRequest(
            @NotBlank String service,
            @NotBlank String name,
            @NotNull Double value,
            Map<String, String> tags) {
    }

    /**
     * Where Kafka stored the record, returned once the brokers have acknowledged it: the value exactly
     * as Kafka stores it ({@code sent}), and which parts were removed or encrypted on the way.
     */
    public record PublishResponse(String id, String topic, int partition, long offset, Object sent,
                                  List<String> removedValues, List<String> encryptedValues) {
    }

    /** A sensitive Kafka key is rejected by PciKafkaTemplate: report it as a client error. */
    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail rejected(IllegalArgumentException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    @PostMapping("/logs")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public CompletableFuture<PublishResponse> publishLog(@Valid @RequestBody LogRequest request) {
        var message = LogMessage.of(request.service(), request.level(), request.message());
        return publisher.publish(message).thenApply(result -> toResponse(message.id(), result));
    }

    @PostMapping("/events")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public CompletableFuture<PublishResponse> publishEvent(@Valid @RequestBody EventRequest request) {
        var payload = request.payload() == null ? Map.<String, Object>of() : request.payload();
        var event = AppEvent.of(request.type(), request.aggregateId(), payload);
        return publisher.publish(event).thenApply(result -> toResponse(event.id(), result));
    }

    @PostMapping("/metrics")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public CompletableFuture<PublishResponse> publishMetric(@Valid @RequestBody MetricRequest request) {
        var tags = request.tags() == null ? Map.<String, String>of() : request.tags();
        var metric = MetricMessage.of(request.service(), request.name(), request.value(), tags);
        return publisher.publish(metric).thenApply(result -> toResponse(metric.id(), result));
    }

    private static PublishResponse toResponse(String id, PciSendResult result) {
        var meta = result.sendResult().getRecordMetadata();
        return new PublishResponse(id, meta.topic(), meta.partition(), meta.offset(), result.sentValue(),
                result.removedValues(), result.encryptedValues());
    }
}
