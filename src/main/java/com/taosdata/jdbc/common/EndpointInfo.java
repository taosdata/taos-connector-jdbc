package com.taosdata.jdbc.common;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class EndpointInfo {
    private AtomicBoolean online;
    private AtomicInteger connectCount;
    public EndpointInfo() {
        this.online = new AtomicBoolean(true);
        this.connectCount = new AtomicInteger(0);
    }

    public boolean isOnline() {
        return online.get();
    }

    public int getConnectCount() {
        return connectCount.get();
    }

    public void setOnline(boolean online) {
        this.online.set(online);
    }

    public void incrementConnectCount() {
        this.connectCount.incrementAndGet();
    }

    public void decrementConnectCount() {
        this.connectCount.decrementAndGet();
    }

}
