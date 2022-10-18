package org.cjm.logging;

import java.util.concurrent.atomic.AtomicInteger;

public class PrintUitls {
    private static final String MSG = "=======> 【num】 time : 【time】;msg : 【【msg】】";
    private static final AtomicInteger num = new AtomicInteger(0);
    /**
     * DateTimeFormatter formatter=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
     */
    public static void printToConsole(String msg) {
        int times = num.getAndIncrement();
        String threadAndUserMsg = getMsg(msg);
        String m = MSG.replace("【time】", getTime());
        m = m.replace("【msg】", threadAndUserMsg);
        m = m.replace("【num】", String.valueOf(times));
        ThreadLog.putMsg(m);
    }

    private static String getMsg(String msg) {
        return ThreadUtils.getThreadInfo() + ";user msg = 【" + msg + "】";
    }

    private static String getTime() {
//        DateTimeFormatter formatter=DateTimeFormatter.ISO_DATE_TIME;
//        LocalDateTime currentTime = TimeUtils.getCurrentTime();
//        String time = currentTime.format(formatter);
        return String.valueOf(System.nanoTime());
    }
}
