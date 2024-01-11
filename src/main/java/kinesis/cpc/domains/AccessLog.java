package kinesis.cpc.domains;

import java.time.LocalDateTime;

import org.apache.flink.api.java.functions.KeySelector;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@RequiredArgsConstructor
@ToString
public class AccessLog {
  private String ipAddress;
  private String url;
  private String referer;
  private String memberId;
  private String userAgent;
  private LocalDateTime localDateTime;

    public static KeySelector<AccessLog, String> getKeySelector() {
      return accessLog -> accessLog.ipAddress;
  }
}
