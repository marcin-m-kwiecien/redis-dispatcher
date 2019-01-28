package solutions.boono.redisdispatcher.publisher;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import solutions.boono.redisdispatcher.Message;
import solutions.boono.redisdispatcher.MessageSerializer;
import solutions.boono.redisdispatcher.Settings;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class Publisher<T> {
    private static final int DEFAULT_DELAY = 100;
    private static final long SUCCESS_RESPONSE = 1L;
    private static final String ELECTIONS_CHANNEL = "elections";
    private static final String ELECTIONS_IN_PROGRESS_PREFIX = "electionsInProgress:";
    private static final String SUBSCRIPTIONS_KEY_PREFIX = "subscriptions:";

    private final MessageSerializer<T> messageSerializer;
    private final JedisPool jedisPool;
    private final DelayQueue<DelayedMessage<T>> failedMessages = new DelayQueue<>();
    private boolean working = false;

    public Publisher(Settings settings, MessageSerializer<T> messageSerializer) {
        this.messageSerializer = messageSerializer;
        this.jedisPool = new JedisPool(settings.getRedisHost(), settings.getRedisPort());
    }

    public void initialize() {
        this.working = true;
        new Thread(this::retry).start();
    }

    public void stop() {
        this.working = false;
        jedisPool.close();
    }

    public void publish(Message<T> message) {
        String key = SUBSCRIPTIONS_KEY_PREFIX + message.getTopic();
        String electionsInProgressKey = ELECTIONS_IN_PROGRESS_PREFIX + message.getTopic();

        Jedis resource = jedisPool.getResource();

        String serialized = this.messageSerializer.serialize(message.getContent());
        Long receivers = resource.publish(message.getTopic(), serialized);
        if (receivers != 0) {
            resource.del(electionsInProgressKey);
            resource.close();
            return;
        }

        failedMessages.add(new DelayedMessage<>(message, DEFAULT_DELAY));

        long result = resource.setnx(electionsInProgressKey, "true");
        if (result == SUCCESS_RESPONSE) {
            System.out.println("Deleted key: " + key);
            resource.del(key);
            resource.publish(ELECTIONS_CHANNEL, message.getTopic());
        }
        resource.close();
    }

    private void retry() {
        try {
            while (working) {
                DelayedMessage<T> msg = failedMessages.poll(DEFAULT_DELAY, TimeUnit.MILLISECONDS);
                if (msg != null) {
                    publish(msg.message);
                }
            }
        } catch (InterruptedException ignored) {
        }
    }

    private static class DelayedMessage<T> implements Delayed {
        private final Message<T> message;
        private final long delayMs;

        private DelayedMessage(Message<T> message, long delayMs) {
            this.message = message;
            this.delayMs = delayMs;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.toMillis(1) * delayMs;
        }

        @Override
        public int compareTo(Delayed o) {
            return (int) (delayMs - o.getDelay(TimeUnit.MILLISECONDS));
        }
    }
}
