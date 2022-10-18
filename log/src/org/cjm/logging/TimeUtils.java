package org.cjm.logging;

import java.time.LocalDateTime;
import java.time.ZoneId;

public class TimeUtils {
    public static LocalDateTime getCurrentTime() {
        return LocalDateTime.now(ZoneId.of("Asia/Shanghai"));
    }
}
