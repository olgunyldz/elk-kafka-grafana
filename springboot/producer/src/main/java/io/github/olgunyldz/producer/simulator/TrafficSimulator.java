package io.github.olgunyldz.producer.simulator;

import io.github.olgunyldz.producer.model.AppEvent;
import io.github.olgunyldz.producer.model.LogMessage;
import io.github.olgunyldz.producer.service.KafkaPublisher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Publishes random logs and order events so there is steady traffic to look at.
 * Off by default; turn it on with {@code app.simulator.enabled=true}.
 */
@Component
@ConditionalOnBooleanProperty("app.simulator.enabled")
public class TrafficSimulator {

    private static final List<String> SERVICES = List.of("order-service", "payment-service", "user-service");
    private static final List<String> EVENT_TYPES = List.of("OrderPlaced", "OrderPaid", "OrderShipped");

    private final KafkaPublisher publisher;

    public TrafficSimulator(KafkaPublisher publisher) {
        this.publisher = publisher;
    }

    @Scheduled(fixedRateString = "${app.simulator.interval:1s}")
    void tick() {
        var random = ThreadLocalRandom.current();
        var service = SERVICES.get(random.nextInt(SERVICES.size()));
        publisher.publish(LogMessage.of(service, randomLevel(random.nextInt(100)), "Handled request #" + random.nextInt(10_000)));

        var orderId = "order-" + random.nextInt(1, 21);
        var type = EVENT_TYPES.get(random.nextInt(EVENT_TYPES.size()));
        publisher.publish(AppEvent.of(type, orderId, Map.of("amount", random.nextInt(10, 500))));
    }

    // Roughly 70% INFO, 15% DEBUG, 10% WARN, 5% ERROR
    private static String randomLevel(int roll) {
        if (roll < 70) return "INFO";
        if (roll < 85) return "DEBUG";
        if (roll < 95) return "WARN";
        return "ERROR";
    }
}
