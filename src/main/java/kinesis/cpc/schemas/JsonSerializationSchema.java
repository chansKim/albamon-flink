package kinesis.cpc.schemas;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import kinesis.cpc.config.FlinkConfig;

public class JsonSerializationSchema<T> implements SerializationSchema<T> {

    @Override
    public byte[] serialize(T item) {
        try {
            return FlinkConfig.objectMapper().writeValueAsBytes(item);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(String.format("Could not serialize value '%s'.", item), e);
        }
    }
}