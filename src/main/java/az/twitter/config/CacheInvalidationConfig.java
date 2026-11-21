package az.twitter.config;

import az.twitter.service.CacheInvalidationService;

import java.nio.charset.StandardCharsets;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Subscribes this instance to the L1 invalidation channel. The container holds one dedicated Redis connection
 * for the subscription and resubscribes by itself after Redis comes back.
 */
@Configuration
public class CacheInvalidationConfig {

    @Bean
    public RedisMessageListenerContainer cacheInvalidationListenerContainer(RedisConnectionFactory connectionFactory,
                                                                            CacheInvalidationService invalidation) {
        var container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(
                (message, pattern) -> invalidation.onMessage(new String(message.getBody(), StandardCharsets.UTF_8)),
                new ChannelTopic(CacheInvalidationService.CHANNEL));
        return container;
    }
}
