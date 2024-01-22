package kinesis.cpc.config;

import java.util.Arrays;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import kinesis.cpc.domains.AccessLog;

public class Splitter implements FlatMapFunction<String, AccessLog> {
		@Override
		public void flatMap(String input, Collector out) throws Exception {
			Arrays.stream(FlinkConfig.objectMapper().readValue(input, AccessLog[].class))
					.forEach(value -> out.collect(AccessLog.of(value)));
		}
	}