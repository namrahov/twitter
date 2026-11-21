package az.twitter.controller;

import az.twitter.worker.DeadLetterReplayer;

import java.util.Map;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Operator switch for replaying dead letters. No auth yet (like the rest of the API): in a real deployment this
 * would sit behind an admin role or not be exposed publicly at all.
 */
@RestController
@RequestMapping("/admin/dlt/replay")
public class AdminDltController {

    private final KafkaListenerEndpointRegistry registry;

    public AdminDltController(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @PostMapping("/start")
    public Map<String, Object> start() {
        container().start();
        return status();
    }

    @PostMapping("/stop")
    public Map<String, Object> stop() {
        container().stop();
        return status();
    }

    @GetMapping
    public Map<String, Object> status() {
        return Map.of("running", container().isRunning());
    }

    private MessageListenerContainer container() {
        return registry.getListenerContainer(DeadLetterReplayer.LISTENER_ID);
    }
}
