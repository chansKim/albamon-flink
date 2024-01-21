package kinesis.cpc.config;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.kinesis.shaded.com.amazonaws.regions.Regions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import lombok.experimental.UtilityClass;

@UtilityClass
public class FlinkConfig {
	public final String DEFAULT_SOURCE_STREAM = "jmchoi1-clickstream-poc";
	public final String DEFAULT_PUBLISHER_TYPE = RecordPublisherType.POLLING.name(); // "POLLING" for standard consumer, "EFO" for Enhanced Fan-Out
	public final String DEFAULT_EFO_CONSUMER_NAME = "albamon-flink-consumer";
	public final String DEFAULT_SINK_STREAM = "chansKinesis";
	public final String DEFAULT_AWS_REGION = Regions.AP_NORTHEAST_2.getName();
	public final String DEFAULT_FILTER_AGENT = "Postman,Etc";

	public final long DEDUPE_CACHE_EXPIRATION_TIME_MS = 30_000;

	/**
	 * Get configuration properties from Amazon Managed Service for Apache Flink runtime properties
	 * GroupID "FlinkApplicationProperties", or from command line parameters when running locally
	 */
	public static ParameterTool loadApplicationParameters(String[] args, StreamExecutionEnvironment env) throws IOException {
		if (env instanceof LocalStreamEnvironment) {
			return ParameterTool.fromArgs(args);
		} else {
			Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
			Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");
			if (flinkProperties == null) {
				throw new RuntimeException("Unable to load FlinkApplicationProperties properties from runtime properties");
			}
			Map<String, String> map = new HashMap<>(flinkProperties.size());
			flinkProperties.forEach((k, v) -> map.put((String)k, (String)v));
			return ParameterTool.fromMap(map);
		}
	}

	public ObjectMapper objectMapper() {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.registerModule(new JavaTimeModule());
		objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
		objectMapper.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));
		return objectMapper;
	}
}