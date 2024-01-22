package kinesis.cpc.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.flink.api.java.utils.ParameterTool;
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
	private ObjectMapper objectMapper;

	static {
		objectMapper = new ObjectMapper();
		objectMapper.registerModule(new JavaTimeModule());
		objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
		objectMapper.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));
	}

	/**
	 * Get configuration properties from Amazon Managed Service for Apache Flink runtime properties
	 * GroupID "FlinkApplicationProperties", or from command line parameters when running locally
	 */
	public ParameterTool loadApplicationParameters(String[] args, StreamExecutionEnvironment env) throws IOException {
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
		return objectMapper;
	}
}