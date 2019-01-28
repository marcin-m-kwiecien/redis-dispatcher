package solutions.boono.redisdispatcher.consumer;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import solutions.boono.redisdispatcher.Message;
import solutions.boono.redisdispatcher.MessageDeserializer;
import solutions.boono.redisdispatcher.Settings;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class Subscriber<T> extends JedisPubSub {
    private static final String ELECTIONS_CHANNEL = "elections";
    private static final String SUBSCRIPTIONS_KEY_PREFIX = "subscriptions:";
    private static final Long SUCCESS_RESPONSE = 1L;

    private final UUID consumerId = UUID.randomUUID();

    private final JedisPool jedisPool;
    private final Map<String, JedisPubSub> pubsubs = new ConcurrentHashMap<>();
    private final MessageDeserializer<T> messageDeserializer;
    private Consumer<Message<T>> messageConsumer;

    public Subscriber(Settings settings, MessageDeserializer<T> messageDeserializer) {
        this.messageDeserializer = messageDeserializer;
        this.jedisPool = new JedisPool(settings.getRedisHost(), settings.getRedisPort());
        this.messageConsumer = m -> {
        };
    }

    public Subscriber(Settings settings, MessageDeserializer<T> messageDeserializer, Consumer<Message<T>> messageConsumer) {
        this.messageDeserializer = messageDeserializer;
        this.jedisPool = new JedisPool(settings.getRedisHost(), settings.getRedisPort());
        this.messageConsumer = messageConsumer;
    }

    public void initialize() {
        new Thread(() -> {
            Jedis resource = jedisPool.getResource();
            pubsubs.put(ELECTIONS_CHANNEL, this);
            resource.subscribe(this, ELECTIONS_CHANNEL);
        }).start();
    }

    public void stop() {
        this.pubsubs.values().forEach(JedisPubSub::unsubscribe);
    }

    public void setMessageConsumer(Consumer<Message<T>> messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    @Override
    public void onMessage(String channel, String message) {
        String key = SUBSCRIPTIONS_KEY_PREFIX + message;
        Jedis resource = jedisPool.getResource();
        if (!resource.exists(key)) {
            Long result = resource.setnx(key, consumerId.toString());
            if (SUCCESS_RESPONSE.equals(result)) {
                subscribeSucceeded(message);
            }
        }
        resource.close();
    }

    private void subscribeSucceeded(String topic) {
        System.out.println("Subscribed at " + topic);
        new Thread(() -> {
            Jedis resource = jedisPool.getResource();
            JedisPubSub pubsub = new SubscriptionHandler();
            pubsubs.put(topic, pubsub);
            resource.subscribe(pubsub, topic);
        }).start();
    }

    private class SubscriptionHandler extends JedisPubSub {
        @Override
        public void onMessage(String channel, String message) {
            Optional<T> deserialize = messageDeserializer.deserialize(message);
            if (deserialize.isPresent()) {
                Message<T> msg = new Message<>(channel, deserialize.get());
                messageConsumer.accept(msg);
            }
        }
    }
}
