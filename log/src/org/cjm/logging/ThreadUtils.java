package org.cjm.logging;

public class ThreadUtils {
    public static String getThreadInfo() {
        Thread thread = Thread.currentThread();
        String name = thread.getName();
        StackTraceElement[] stackTrace = thread.getStackTrace();
        StackTraceElement stackTraceElement = stackTrace[4];
        return "thread name = " + name + ";location=【" +
                stackTraceElement.getClassName() + "#" + stackTraceElement.getMethodName() +
                ";lineNumber=" + stackTraceElement.getLineNumber() + "】";
    }
}
