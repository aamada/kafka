/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class PrintUitls {
    static {
        File file = new File("D:\\kafka.1.log");
        if (file.exists()) {
            file.delete();
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    private static final String MSG = "=======> 【num】;time : 【time】;\r\nmsg : \r\n 【【msg】】";
    private static final AtomicInteger NUM = new AtomicInteger(0);
    private static File FILE = new File("D:\\kafka.1.log");
    /**
     * DateTimeFormatter formatter=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
     */
    public synchronized static void printToConsole(String msg) {
        int times = NUM.getAndIncrement();
        String threadAndUserMsg = getMsg(msg);
        String m = MSG.replace("【time】", getTime());
        m = m.replace("【msg】", threadAndUserMsg);
        m = m.replace("【num】", String.valueOf(times));
        printConsoleFile(m);
//        System.err.println(m);
    }

    private static void printConsoleFile(String m) {

        byte[] bytes;
        try {
            bytes = ("\r\n" + m).getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(FILE, true))) {
            outputStream.write(bytes);
            outputStream.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String getMsg(String msg) {
        return getThreadInfo() + "; \r\nuser msg = 【" + msg + "】";
    }

    private static String getTime() {
        return String.valueOf(System.nanoTime());
    }

    public static String getThreadInfo() {
        Thread thread = Thread.currentThread();
        String name = thread.getName();
        long id = thread.getId();
        StackTraceElement[] stackTrace = thread.getStackTrace();
        StackTraceElement stackTraceElement = stackTrace[4];
        return "thread name = " + name + ";id = "+id+";location=【" +
                stackTraceElement.getClassName() + "#" + stackTraceElement.getMethodName() +
                ";lineNumber=" + stackTraceElement.getLineNumber() + "】";
    }
}
