package com.taosdata.jdbc.utils;

public class TestEnvUtil {
    private static final String HOST = System.getenv().get("TDENGINE_HOST");
    private static final int JNI_PORT = Integer.parseInt(System.getenv().get("TDENGINE_JNI_PORT"));
    private static final int RS_PORT = Integer.parseInt(System.getenv().get("TDENGINE_RS_PORT"));
    private static final int WS_PORT = Integer.parseInt(System.getenv().get("TDENGINE_WS_PORT"));
    private static final String USER = System.getenv().get("TDENGINE_USER");
    private static final String PASSWORD = System.getenv().get("TDENGINE_PASSWORD");

    private TestEnvUtil() {
    }

    public static String getHost() {
        return HOST;
    }
    public static int getJniPort() {
        return JNI_PORT;
    }
    public static int getRsPort() {
        return RS_PORT;
    }
    public static int getWsPort() {
        return WS_PORT;
    }

    public static String getUser() {
        return USER;
    }
    public static String getPassword() {
        return PASSWORD;
    }
}
