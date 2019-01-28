package solutions.boono.redisdispatcher;

public interface MessageSerializer<T> {
    String serialize(T object);
}
