package solutions.boono.redisdispatcher.tests;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import solutions.boono.redisdispatcher.Message;
import solutions.boono.redisdispatcher.MessageDeserializer;
import solutions.boono.redisdispatcher.Settings;
import solutions.boono.redisdispatcher.consumer.Subscriber;
import solutions.boono.redisdispatcher.publisher.Publisher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DispatcherTest {
    private final Settings settings;

    DispatcherTest() throws IOException {
        Properties properties = new Properties();
        settings = new Settings(properties);
    }

    @RepeatedTest(100)
    void dispatcherHeavyTest() throws InterruptedException {
        Jedis jedis = new Jedis(settings.getRedisHost(), settings.getRedisPort());
        Set<String> keys = jedis.keys("*");
        for (String key : keys) {
            jedis.del(key);
        }

        JacksonSerializerDeserializer serializer = new JacksonSerializerDeserializer();
        Publisher<TestMessage> pub = new Publisher<>(settings, serializer);
        List<TestSubscriber> subscribers = IntStream
                .range(0, 25)
                .mapToObj(i -> new TestSubscriber(settings, serializer))
                .collect(Collectors.toList());

        pub.initialize();
        subscribers.forEach(Subscriber::initialize);

        List<String> topics = IntStream
                .range(0, 100)
                .mapToObj(i -> "/msg/" + i + "topic")
                .collect(Collectors.toList());

        Supplier<String> randTopic = () -> topics.get(ThreadLocalRandom.current().nextInt(topics.size()));

        int messagesCount = 8000;
        int threadCount = 8;
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < threadCount; t++) {
            Thread thread = new Thread(() -> {
                for (int i = 0; i < messagesCount / threadCount; i++) {
                    TestMessage message = new TestMessage();
                    message.setField1("Message" + i);
                    message.setField2(i);
                    pub.publish(new Message<>(randTopic.get(), message));
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException ignored) {

                    }
                }
            });
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }
        Thread.sleep(1000);

        topics.forEach(t -> jedis.del("subscriptions/" + t));

        pub.stop();
        subscribers.forEach(Subscriber::stop);

        assertEquals(messagesCount, subscribers.stream().mapToInt(s -> s.consumedMessages.get()).sum());
    }

    @Test
    void dispatcherTest() throws InterruptedException {
        JacksonSerializerDeserializer serializer = new JacksonSerializerDeserializer();
        Publisher<TestMessage> publisher = new Publisher<>(settings, serializer);
        Subscriber<TestMessage> subscriber1 = new Subscriber<>(settings, serializer, msg -> System.out.println("S1: " + msg.getContent().toString()));
        Subscriber<TestMessage> subscriber2 = new Subscriber<>(settings, serializer, msg -> System.out.println("S2: " + msg.getContent().toString()));

        publisher.initialize();
        subscriber1.initialize();
        subscriber2.initialize();

        TestMessage testMessage = new TestMessage();
        testMessage.setField1("abcdef");
        testMessage.setField2(15);
        publisher.publish(new Message<>("/msg/abcdef", testMessage));
        testMessage = new TestMessage();
        testMessage.setField1("abcdefg");
        testMessage.setField2(16);
        publisher.publish(new Message<>("/msg/abcdefg", testMessage));
        testMessage = new TestMessage();
        testMessage.setField1("abcdef");
        testMessage.setField2(17);
        publisher.publish(new Message<>("/msg/abcdef", testMessage));
        testMessage = new TestMessage();
        testMessage.setField1("abcdefg");
        testMessage.setField2(18);
        publisher.publish(new Message<>("/msg/abcdefg", testMessage));
        testMessage = new TestMessage();
        testMessage.setField1("abcdefg");
        testMessage.setField2(19);
        publisher.publish(new Message<>("/msg/abcdefg", testMessage));
        testMessage = new TestMessage();
        testMessage.setField1("abcdefg");
        testMessage.setField2(20);
        publisher.publish(new Message<>("/msg/abcdefg", testMessage));
        testMessage = new TestMessage();
        testMessage.setField1("abcdefg");
        testMessage.setField2(21);
        publisher.publish(new Message<>("/msg/abcdefg", testMessage));
        testMessage = new TestMessage();
        testMessage.setField1("abcdefg");
        testMessage.setField2(22);
        publisher.publish(new Message<>("/msg/abcdefg", testMessage));
        testMessage = new TestMessage();
        testMessage.setField1("abcdefg");
        testMessage.setField2(23);
        publisher.publish(new Message<>("/msg/abcdefg", testMessage));

        Thread.sleep(1000);

        publisher.stop();
        subscriber1.stop();
        subscriber2.stop();
        Jedis jedis = new Jedis(settings.getRedisHost(), settings.getRedisPort());
        jedis.del("subscriptions/msg/abcdef");
        jedis.del("subscriptions/msg/abcdefg");
    }

    private static class TestSubscriber extends Subscriber<TestMessage> {
        private Set<String> topics = new ConcurrentSkipListSet<>();
        private AtomicInteger consumedMessages = new AtomicInteger(0);

        TestSubscriber(Settings settings, MessageDeserializer<TestMessage> messageDeserializer) {
            super(settings, messageDeserializer);
            setMessageConsumer(this::consume);
        }

        private void consume(Message<TestMessage> testMessage) {
            topics.add(testMessage.getTopic());
            consumedMessages.incrementAndGet();
        }
    }
}
