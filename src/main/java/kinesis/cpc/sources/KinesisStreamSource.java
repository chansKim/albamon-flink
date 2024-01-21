package kinesis.cpc.sources;

import static kinesis.cpc.config.FlinkConfig.*;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.*;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.*;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import lombok.experimental.UtilityClass;

@UtilityClass
public class KinesisStreamSource {

	public FlinkKinesisConsumer<String> createKinesisSource(
			ParameterTool applicationProperties) {

		// Properties for Amazon Kinesis Data Streams Source, we need to specify from where we want to consume the data.
		// STREAM_INITIAL_POSITION: LATEST: consume messages that have arrived from the moment application has been deployed
		// STREAM_INITIAL_POSITION: TRIM_HORIZON: consume messages starting from first available in the Kinesis Stream
		Properties kinesisConsumerConfig = new Properties();
		kinesisConsumerConfig.put(AWSConfigConstants.AWS_REGION, applicationProperties.get("kinesis.region", DEFAULT_AWS_REGION));
		kinesisConsumerConfig.put(STREAM_INITIAL_POSITION, "LATEST");

		// Set up publisher type: POLLING (standard consumer) or EFO (Enhanced Fan-Out)
		kinesisConsumerConfig.put(RECORD_PUBLISHER_TYPE, applicationProperties.get("kinesis.source.type", DEFAULT_PUBLISHER_TYPE));
		if (kinesisConsumerConfig.getProperty(RECORD_PUBLISHER_TYPE).equals(EFO.name())) {
			kinesisConsumerConfig.put(ConsumerConfigConstants.EFO_CONSUMER_NAME, applicationProperties.get("kinesis.source.efoConsumer", DEFAULT_EFO_CONSUMER_NAME));
		}

		return new FlinkKinesisConsumer<>(applicationProperties.get("kinesis.source.stream", DEFAULT_SOURCE_STREAM), new SimpleStringSchema(), kinesisConsumerConfig);
	}
}