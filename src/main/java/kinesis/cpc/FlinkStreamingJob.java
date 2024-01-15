package kinesis.cpc;

import static kinesis.cpc.constants.FlinkConstants.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import kinesis.cpc.constants.FlinkConstants;
import kinesis.cpc.domains.AccessLog;
import kinesis.cpc.filters.DedupeFilterFunction;
import kinesis.cpc.sinks.KinesisStreamSink;
import kinesis.cpc.sources.KinesisStreamSource;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class FlinkStreamingJob {

	/**
	 * Get configuration properties from Amazon Managed Service for Apache Flink runtime properties
	 * GroupID "FlinkApplicationProperties", or from command line parameters when running locally
	 */
	private static ParameterTool loadApplicationParameters(String[] args, StreamExecutionEnvironment env) throws IOException {
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

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		final ParameterTool applicationProperties = loadApplicationParameters(args, env);
		log.warn("Application properties: {}", applicationProperties.toMap());

		FlinkKinesisConsumer<String> source = KinesisStreamSource.createKinesisSource(applicationProperties);
		DataStream<String> input = env.addSource(source, "Kinesis source");

		input.map(value -> FlinkConstants.objectMapper().readValue(value, AccessLog.class))
				.keyBy(v -> v.getIpAddress()) // Logically partition the stream per stock symbol
				.filter(new DedupeFilterFunction(AccessLog.getKeySelector(), DEDUPE_CACHE_EXPIRATION_TIME_MS))
				.sinkTo(KinesisStreamSink.createKinesisSink(applicationProperties)); // Write to Firehose Delivery Stream

		env.execute();
	}
}