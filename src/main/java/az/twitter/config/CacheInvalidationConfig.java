package az.twitter.config;

import az.twitter.service.CacheInvalidationService;

import java.nio.charset.StandardCharsets;

import io.lettuce.core.RedisChannelHandler;
import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Subscribes this instance to the L1 invalidation channel and to its own heartbeat channel. The container holds
 * one dedicated Redis connection for both, so a ping arriving proves invalidations can arrive too.
 */
@Configuration
public class CacheInvalidationConfig {

    private final CacheInvalidationService invalidation;

    public CacheInvalidationConfig(CacheInvalidationService invalidation) {
        this.invalidation = invalidation;
    }

    @Bean
    public RedisMessageListenerContainer cacheInvalidationListenerContainer(RedisConnectionFactory connectionFactory) {
        var container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(
                (message, pattern) -> invalidation.onMessage(new String(message.getBody(), StandardCharsets.UTF_8)),
                new ChannelTopic(CacheInvalidationService.CHANNEL));
        container.addMessageListener((message, pattern) -> invalidation.onPing(),
                new ChannelTopic(invalidation.pingChannel()));
        return container;
    }

    /**
     * Lettuce reconnects and resubscribes a dropped Pub/Sub connection by itself, silently: without this the app
     * would not know that messages were lost meanwhile. Registered once the client exists (after startup).
     */
    @EventListener(ApplicationStartedEvent.class)
    public void watchSubscriptionConnection(ApplicationStartedEvent event) {
        RedisConnectionFactory factory = event.getApplicationContext().getBean(RedisConnectionFactory.class);
        if (factory instanceof LettuceConnectionFactory lettuce) {
            lettuce.getRequiredNativeClient().addListener(new RedisConnectionStateListener() {
                @Override
                public void onRedisDisconnected(RedisChannelHandler<?, ?> connection) {
                    if (connection instanceof StatefulRedisPubSubConnection<?, ?>) {
                        invalidation.onSubscriptionLost();
                    }
                }
            });
        }
    }
}
