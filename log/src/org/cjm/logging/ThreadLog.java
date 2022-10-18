package org.cjm.logging;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ThreadLog {
    private static final BlockingQueue<String> QUEUE = new ArrayBlockingQueue<String>(1000000);
    public static void putMsg(String msg) {
        try {
            QUEUE.put(msg);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static {
        Thread t = new Thread(() -> {
            while (true) {
                String o = "";
                try {
                    o = QUEUE.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.err.println(o);
            }
        });
        t.setDaemon(true);
        t.setName("cjm-log-thread");
        t.start();
    }
}
