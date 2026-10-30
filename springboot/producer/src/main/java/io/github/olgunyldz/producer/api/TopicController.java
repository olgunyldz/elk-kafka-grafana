package io.github.olgunyldz.producer.api;

import io.github.olgunyldz.producer.config.TopicProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Data for the web page: the topic dropdown and the sample buttons. */
@RestController
@RequestMapping("/api")
public class TopicController {

    /**
     * @param name     Kafka topic name
     * @param type     message type, which picks the sample data on the page: log, event or metric
     * @param endpoint where to POST a message for this topic
     */
    public record TopicInfo(String name, String type, String endpoint) {
    }

    private final TopicProperties topics;
    private final SampleMessages samples;

    public TopicController(TopicProperties topics, SampleMessages samples) {
        this.topics = topics;
        this.samples = samples;
    }

    @GetMapping("/topics")
    public List<TopicInfo> topics() {
        return List.of(
                new TopicInfo(topics.logs(), "log", "/api/logs"),
                new TopicInfo(topics.events(), "event", "/api/events"),
                new TopicInfo(topics.metrics(), "metric", "/api/metrics"));
    }

    @GetMapping("/samples")
    public List<SampleMessages.Sample> samples() {
        return samples.all();
    }
}
