package solutions.boono.redisdispatcher;

import java.util.Optional;

public interface MessageDeserializer<T> {
    Optional<T> deserialize(String serialized);
}
