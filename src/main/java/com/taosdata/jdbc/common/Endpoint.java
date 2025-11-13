package com.taosdata.jdbc.common;

public class Endpoint {
    private final String host;
    private final int port;
    private final boolean isIpv6;
    public Endpoint(String host, int port, boolean isIpv6) {
        this.host = host;
        this.port = port;
        this.isIpv6 = isIpv6;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isIpv6() {
        return isIpv6;
    }
}
