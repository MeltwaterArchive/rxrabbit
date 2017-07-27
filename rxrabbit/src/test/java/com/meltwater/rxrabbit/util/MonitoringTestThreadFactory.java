package com.meltwater.rxrabbit.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadFactory;

public class MonitoringTestThreadFactory implements ThreadFactory {

    private final List<Thread> createdThreads = Collections.synchronizedList(new ArrayList<>());

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "MonitoringTestThreadFactory-" + createdThreads.size());
        t.setDaemon(true);
        createdThreads.add(t);
        return t;
    }

    public int getAliveThreads() {
        int alive = 0;
        for (Thread createdThread : createdThreads) {
            if (createdThread.isAlive()) {
                alive++;
            }
        }
        return alive;
    }
}
