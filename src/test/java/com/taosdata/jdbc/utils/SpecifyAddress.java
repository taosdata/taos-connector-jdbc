package com.taosdata.jdbc.utils;

public class SpecifyAddress {
    private static int JNI_PORT_DEFAULT = 6030;
    private static int REST_PORT_DEFAULT = 6041;
    private static int WEB_SOCKET_PORT_DEFAULT = 6041;
    private String JNI_URL = null;
    private String JNI_WITHOUT_PROP_URL = null;
    private String HOST = null;
    private String JNI_PORT = null;
    private String REST_URL = null;
    private String REST_WITHOUT_PROP_URL = null;
    private String REST_PORT = null;

    private String WEB_SOCKET_URL = null;
    private String WEB_SOCKET_WITHOUT_PROP_URL = null;
    private String WEB_SOCKET_PORT = null;
    private SpecifyAddress() {
        String host = System.getProperty("maven.test.host");
        if (null != host && !"".equals(host.trim())) {
            HOST = host.trim();
            String jni = System.getProperty("maven.test.port.jni");
            if (null != jni && !"".equals(jni.trim())) {
                JNI_URL = "jdbc:TAOS://" + host.trim() + ":" + jni.trim() + "/?user=root&password=taosdata";
                JNI_WITHOUT_PROP_URL = "jdbc:TAOS://" + host.trim() + ":" + jni.trim() + "/";

                JNI_PORT = jni.trim();
            } else {
                JNI_URL = "jdbc:TAOS://" + host.trim() + ":" + JNI_PORT_DEFAULT + "/?user=root&password=taosdata";
                JNI_WITHOUT_PROP_URL = "jdbc:TAOS://" + host.trim() + ":" + JNI_PORT_DEFAULT + "/";
                JNI_PORT = JNI_PORT_DEFAULT + "";
            }
            String rest = System.getProperty("maven.test.port.rest");
            if (null != rest && !"".equals(rest.trim())) {
                REST_URL = "jdbc:TAOS-RS://" + host.trim() + ":" + rest.trim() + "/?user=root&password=taosdata";
                REST_WITHOUT_PROP_URL = "jdbc:TAOS-RS://" + host.trim() + ":" + rest.trim() + "/";
                REST_PORT = rest.trim();
            } else {
                REST_URL = "jdbc:TAOS-RS://" + host.trim() + ":" + REST_PORT_DEFAULT + "/?user=root&password=taosdata";
                REST_WITHOUT_PROP_URL = "jdbc:TAOS-RS://" + host.trim() + ":" + REST_PORT_DEFAULT + "/";
                REST_PORT = REST_PORT_DEFAULT + "";
            }



            String websocket = System.getProperty("maven.test.port.websocket");
            if (null != websocket && !"".equals(websocket.trim())) {
                WEB_SOCKET_URL = "jdbc:TAOS-WS://" + host.trim() + ":" + rest.trim() + "/?user=root&password=taosdata";
                WEB_SOCKET_WITHOUT_PROP_URL = "jdbc:TAOS-WS://" + host.trim() + ":" + rest.trim() + "/";
                WEB_SOCKET_PORT = websocket.trim();
            } else {
                WEB_SOCKET_URL = "jdbc:TAOS-WS://" + host.trim() + ":" + WEB_SOCKET_PORT_DEFAULT + "/?user=root&password=taosdata";
                WEB_SOCKET_WITHOUT_PROP_URL = "jdbc:TAOS-WS://" + host.trim() + ":" + WEB_SOCKET_PORT_DEFAULT + "/";
                WEB_SOCKET_PORT = WEB_SOCKET_PORT_DEFAULT + "";
            }
        }
    }

    private static SpecifyAddress instance = new SpecifyAddress();

    public static SpecifyAddress getInstance() {
        return instance;
    }

    public String getJniUrl() {
        return JNI_URL;
    }

    public String getJniWithoutUrl() {
        return JNI_WITHOUT_PROP_URL;
    }

    public String getJniPort() {
        return JNI_PORT;
    }

    public String getRestUrl() {
        return REST_URL;
    }

    public String getRestWithoutUrl() {
        return REST_WITHOUT_PROP_URL;
    }

    public String getRestPort() {
        return REST_PORT;
    }

    public String getHost() {
        return HOST;
    }


    public String getWebSocketUrl() {
        return WEB_SOCKET_URL;
    }

    public String getWebSocketWithoutUrl() {
        return WEB_SOCKET_WITHOUT_PROP_URL;
    }

    public String getWebSocketPort() {
        return WEB_SOCKET_PORT;
    }
}
