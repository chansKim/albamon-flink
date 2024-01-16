package kinesis.cpc.constants;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.*;

import java.util.TimeZone;

import org.apache.flink.kinesis.shaded.com.amazonaws.regions.Regions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import lombok.experimental.UtilityClass;

@UtilityClass
public class FlinkConstants {
	public final String DEFAULT_SOURCE_STREAM = "jmchoi1-clickstream-poc";
	public final String DEFAULT_PUBLISHER_TYPE = RecordPublisherType.POLLING.name(); // "POLLING" for standard consumer, "EFO" for Enhanced Fan-Out
	public final String DEFAULT_EFO_CONSUMER_NAME = "albamon-flink-consumer";
	public final String DEFAULT_SINK_STREAM = "chansKinesis";
	public final String DEFAULT_AWS_REGION = Regions.AP_NORTHEAST_2.getName();
	public final String DEFAULT_FILTER_AGENT = "Postman,Etc";

	public final long DEDUPE_CACHE_EXPIRATION_TIME_MS = 30_000;

	public ObjectMapper objectMapper() {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.registerModule(new JavaTimeModule());
		objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
		objectMapper.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));
		return objectMapper;
	}
}