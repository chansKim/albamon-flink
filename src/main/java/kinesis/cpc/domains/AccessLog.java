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
	private String clientType;
	private String dateTime;

	/**
	 * 서비스 정보
	 */
	private String serviceName;
	private String logType;

	/**
	 * 광고 정보
	 */
	private String recruitNo;
	private String recruitName;
	private String parts;
	private String locations;
	private String products;
	private String layout;

	/**
	 * 사용자 정보
	 */
	private String memberId;
	private String memberType;

	public static AccessLog of(AccessLog log) {
		AccessLog accessLog = new AccessLog();
		accessLog.ipAddress = log.ipAddress;
		accessLog.url = log.url;
		accessLog.referer = log.referer;
		accessLog.userAgent = log.userAgent;
		accessLog.dateTime = log.dateTime;
		accessLog.serviceName = log.serviceName;
		accessLog.logType = log.logType;
		accessLog.recruitNo = log.recruitNo;
		accessLog.memberId = log.memberId;

		return accessLog;
	}

	public static KeySelector<AccessLog, String> getKeySelector() {
		return accessLog -> accessLog.ipAddress + SEPARATOR
							+ accessLog.getRecruitNo() + SEPARATOR
							+ accessLog.logType;
	}
}
