package kinesis.cpc.domains;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@RequiredArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccessLog {
	private static final String SEPARATOR = "/";
	/**
	 * 기본 정보
	 */
	private String ipAddress;
	private String url;
	private String referer;
	private String userAgent;
	private String dateTime;

	/**
	 * 서비스 정보
	 */
	private String serviceName;
	private String logType;

	/**
	 * 사용자 정보
	 */
	private String recruitNo;
	private String memberId;

	public static KeySelector<AccessLog, String> getKeySelector() {
		return accessLog -> accessLog.ipAddress + SEPARATOR
							+ accessLog.getRecruitNo() + SEPARATOR
							+ accessLog.logType;
	}
}
