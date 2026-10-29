package io.github.olgunyldz.producer.api;

import io.github.olgunyldz.producer.config.TopicProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Lists the topics this producer can publish to. Used by the web page's topic dropdown. */
@RestController
@RequestMapping("/api/topics")
public class TopicController {

    /**
     * @param name     Kafka topic name
     * @param type     message type, which picks the sample data on the page: log, event or metric
     * @param endpoint where to POST a message for this topic
     */
    public record TopicInfo(String name, String type, String endpoint) {
    }

    private final TopicProperties topics;

    public TopicController(TopicProperties topics) {
        this.topics = topics;
    }

    @GetMapping
    public List<TopicInfo> list() {
        return List.of(
                new TopicInfo(topics.logs(), "log", "/api/logs"),
                new TopicInfo(topics.events(), "event", "/api/events"),
                new TopicInfo(topics.metrics(), "metric", "/api/metrics"));
    }
}
