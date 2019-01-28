package solutions.boono.redisdispatcher.tests;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import solutions.boono.redisdispatcher.MessageDeserializer;
import solutions.boono.redisdispatcher.MessageSerializer;

import java.io.IOException;
import java.util.Optional;

public class JacksonSerializerDeserializer
        implements MessageSerializer<TestMessage>, MessageDeserializer<TestMessage> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public Optional<TestMessage> deserialize(String serialized) {
        try {
            return Optional.of(OBJECT_MAPPER.readValue(serialized, TestMessage.class));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    @Override
    public String serialize(TestMessage object) {
        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}
