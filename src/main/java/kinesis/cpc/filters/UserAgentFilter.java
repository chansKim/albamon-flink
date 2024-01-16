package kinesis.cpc.filters;

import static java.util.Arrays.*;
import static kinesis.cpc.constants.FlinkConstants.*;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import kinesis.cpc.domains.AccessLog;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class UserAgentFilter {
	public static boolean userAgentFilter(AccessLog accessLog, ParameterTool applicationProperties) {
		if (ObjectUtils.isEmpty(accessLog) || StringUtils.isEmpty(accessLog.getUserAgent())) {
			return false;
		}

		String filterAgents = applicationProperties.get("kinesis.filter.agent", DEFAULT_FILTER_AGENT);
		String[] filterAgentList = filterAgents.split(",");
		if (filterAgentList.length > 1) {
			return stream(filterAgentList).noneMatch(v -> searchKmp(accessLog.getUserAgent(), v));
		}

		return !searchKmp(accessLog.getUserAgent(), filterAgents);
	}

	private static boolean searchKmp(String parent, String pattern) {
		if (StringUtils.isAnyBlank(parent, parent)) {
			return false;
		}

		parent = parent.toLowerCase();
		pattern = pattern.toLowerCase();

		int[] table = makeTable(pattern);

		int n1 = parent.length();
		int n2 = pattern.length();

		int idx = 0;
		for (int i = 0; i < n1; i++) {
			while (idx > 0 && parent.charAt(i) != pattern.charAt(idx)) {
				idx = table[idx - 1];
			}
			if (parent.charAt(i) == pattern.charAt(idx)) {
				if (idx == n2 - 1) {
					return true;
				} else {
					idx += 1;
				}
			}
		}
		return false;
	}

	static int[] makeTable(String pattern) {
		int n = pattern.length();
		int[] table = new int[n];

		int idx = 0;
		for (int i = 1; i < n; i++) {
			while (idx > 0 && pattern.charAt(i) != pattern.charAt(idx)) {
				idx = table[idx - 1];
			}

			if (pattern.charAt(i) == pattern.charAt(idx)) {
				idx += 1;
				table[i] = idx;
			}
		}
		return table;
	}
}