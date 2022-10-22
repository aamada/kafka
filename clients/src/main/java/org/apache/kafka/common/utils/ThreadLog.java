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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThreadLog {
    private static final BlockingQueue<String> QUEUE = new ArrayBlockingQueue<String>(1000000);
    private static final int WAINT_NUM = 20;
    public static void putMsg(String msg) {
        try {
            QUEUE.put(msg);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static {
        File file = new File("D:\\kafka." + System.currentTimeMillis() + ".log");
        try {
            file.createNewFile();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Thread t = new Thread(() -> {
            AtomicBoolean flag = new AtomicBoolean(true);
            long idleTime = 0;
            while (flag.get()) {
                String o = "";
                try {
                    o = QUEUE.poll(WAINT_NUM, TimeUnit.SECONDS);
                } catch (Exception e) {
                    System.err.println("这里注意， 这里只是超时的话呢， 会返回一个null， 但是并不会抛出一个超时异常, 所以这个代码是不会执行的");
                }
                if (null == o) {
                    if (0 == idleTime) {
                        idleTime = System.currentTimeMillis();
                    } else if (System.currentTimeMillis() - idleTime > WAINT_NUM * 1000) {
                        System.err.println("stop log thread");
                        flag.set(false);
                    }
                    continue;
                }
                byte[] bytes = new byte[0];
                try {
                    bytes = ("\r\n" + o).getBytes(StandardCharsets.UTF_8);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file, true))) {
                    outputStream.write(bytes);
                    outputStream.flush();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                idleTime = System.currentTimeMillis();
            }
        });
        t.setName("cjm-log-thread");
        t.start();
    }
}