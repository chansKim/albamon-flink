package kinesis.cpc.sinks;

import static kinesis.cpc.config.FlinkConstants.*;

import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.firehose.sink.KinesisFirehoseSink;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;

import kinesis.cpc.domains.AccessLog;
import kinesis.cpc.schemas.JsonSerializationSchema;
import lombok.experimental.UtilityClass;

@UtilityClass
public class FirehoseSink {
	public KinesisFirehoseSink<AccessLog> createKinesisFirehoseSink(
			ParameterTool applicationProperties) {

		Properties sinkProperties = new Properties();
		// Required
		sinkProperties.put(AWSConfigConstants.AWS_REGION, applicationProperties.get("kinesis.region", DEFAULT_AWS_REGION));

		return KinesisFirehoseSink.<AccessLog>builder()
				.setFirehoseClientProperties(sinkProperties)
				.setSerializationSchema(new JsonSerializationSchema<>())
				.setDeliveryStreamName(applicationProperties.get("kinesis.firehose.sink.stream", DEFAULT_SINK_FIREHOSE_STREAM))
				.build();
	}
}