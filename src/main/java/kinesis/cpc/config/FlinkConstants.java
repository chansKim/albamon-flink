package kinesis.cpc.config;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.*;

import org.apache.flink.kinesis.shaded.com.amazonaws.regions.Regions;

import lombok.experimental.UtilityClass;

@UtilityClass
public class FlinkConstants {

	public final String DEFAULT_SOURCE_STREAM = "raw-stream-kinesis";
	public final String DEFAULT_PUBLISHER_TYPE = RecordPublisherType.POLLING.name(); // "POLLING" for standard consumer, "EFO" for Enhanced Fan-Out
	public final String DEFAULT_EFO_CONSUMER_NAME = "albamon-flink-consumer";
	public final String DEFAULT_SINK_STREAM = "convert-stream-kinesis";
	public final String DEFAULT_AWS_REGION = Regions.AP_NORTHEAST_2.getName();
	public final String DEFAULT_FILTER_AGENT = "Postman,Etc";

	public final long DEDUPE_CACHE_EXPIRATION_TIME_MS = 30_000;
}