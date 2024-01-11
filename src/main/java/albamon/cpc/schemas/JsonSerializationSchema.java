package albamon.cpc.schemas;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

import albamon.cpc.constants.FlinkConstants;

public class JsonSerializationSchema<T> implements SerializationSchema<T> {

    // private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(T item) {
        try {
            return FlinkConstants.objectMapper().writeValueAsBytes(item);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(
                    String.format("Could not serialize value '%s'.", item), e);
        }
    }
}