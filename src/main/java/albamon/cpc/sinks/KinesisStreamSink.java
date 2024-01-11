package albamon.cpc.sinks;

import static albamon.cpc.constants.FlinkConstants.*;

import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;

import albamon.cpc.domains.AccessLog;
import albamon.cpc.schemas.JsonSerializationSchema;
import lombok.experimental.UtilityClass;

@UtilityClass
public class KinesisStreamSink {
	public KinesisStreamsSink<AccessLog> createKinesisSink(
			ParameterTool applicationProperties) {

		Properties sinkProperties = new Properties();
		// Required
		sinkProperties.put(AWSConfigConstants.AWS_REGION, applicationProperties.get("kinesis.region", DEFAULT_AWS_REGION));
		sinkProperties.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKey");
		sinkProperties.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		return KinesisStreamsSink.<AccessLog>builder()
				.setKinesisClientProperties(sinkProperties)
				.setSerializationSchema(new JsonSerializationSchema<>())
				.setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
				.setStreamName(applicationProperties.get("kinesis.sink.stream", DEFAULT_SINK_STREAM))
				.build();
	}
}