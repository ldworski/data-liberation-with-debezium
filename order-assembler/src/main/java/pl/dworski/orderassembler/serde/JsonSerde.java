package pl.dworski.orderassembler.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import pl.dworski.orderassembler.debezium.ChangeEvent;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.json.JsonMapper;

public class JsonSerde<T> implements Serde<T> {

    private final JsonMapper jsonMapper;
    private final JavaType type;

    private JsonSerde(JsonMapper jsonMapper, JavaType type) {
        this.jsonMapper = jsonMapper;
        this.type = type;
    }

    public static <T> JsonSerde<T> of(JsonMapper jsonMapper, Class<T> type) {
        return new JsonSerde<>(jsonMapper, jsonMapper.constructType(type));
    }

    public static <T> JsonSerde<ChangeEvent<T>> changeEventOf(JsonMapper jsonMapper, Class<T> rowType) {
        return new JsonSerde<>(jsonMapper, jsonMapper.getTypeFactory().constructParametricType(ChangeEvent.class, rowType));
    }

    @Override
    public Serializer<T> serializer() {
        return (_, data) -> data == null ? null : jsonMapper.writeValueAsBytes(data);
    }

    @Override
    public Deserializer<T> deserializer() {
        return (_, bytes) -> bytes == null ? null : jsonMapper.readValue(bytes, type);
    }
}
