package solutions.boono.redisdispatcher;

public class Message<T> {
    private final String topic;
    private final T content;

    public Message(String topic, T content) {
        this.topic = topic;
        this.content = content;
    }

    public String getTopic() {
        return topic;
    }

    public T getContent() {
        return content;
    }
}
