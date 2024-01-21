package kinesis.cpc;

import static kinesis.cpc.config.FlinkConfig.*;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;

import kinesis.cpc.domains.AccessLog;
import kinesis.cpc.filters.DedupeFilterFunction;
import kinesis.cpc.filters.UserAgentFilter;
import kinesis.cpc.sinks.KinesisStreamSink;
import kinesis.cpc.sources.KinesisStreamSource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlinkStreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		final ParameterTool applicationProperties = loadApplicationParameters(args, env);
		log.warn("Application properties: {}", applicationProperties.toMap());

		FlinkKinesisConsumer<String> source = KinesisStreamSource.createKinesisSource(applicationProperties);
		DataStream<String> input = env.addSource(source, "Kinesis source");
		analyticsProcess(env, input, applicationProperties);
	}

	private static void analyticsProcess(StreamExecutionEnvironment env, DataStream<String> input, ParameterTool applicationProperties) throws Exception {
		input.map(value -> objectMapper().readValue(value, AccessLog.class))
				.filter(value -> UserAgentFilter.userAgentFilter(value, applicationProperties))
				.filter(new DedupeFilterFunction(AccessLog.getKeySelector(), DEDUPE_CACHE_EXPIRATION_TIME_MS))
				.sinkTo(KinesisStreamSink.createKinesisSink(applicationProperties)); // Write to Firehose Delivery Stream

		env.execute();
	}
}