package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBDriver;

import java.util.HashSet;
import java.util.Set;

public class TMQConstants {
    public static final Set<String> configSet = new HashSet<>();

    public static final String GROUP_ID = "group.id";

    public static final String CLIENT_ID = "client.id";

    /**
     * auto commit default is true then the commitCallback function will be called after 5 seconds
     */
    public static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";

    /**
     * commit interval. unit milliseconds
     */
    public static final String AUTO_COMMIT_INTERVAL = "auto.commit.interval.ms";

    /**
     * only valid in first group id create.
     */
    public static final String AUTO_OFFSET_RESET = "auto.offset.reset";

    /**
     * whether poll result include table name. suggest always true
     */
    public static final String MSG_WITH_TABLE_NAME = "msg.with.table.name";

    /**
     * indicate host and port of connection
     */
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    /**
     * deserializer Bean
     */
    public static final String VALUE_DESERIALIZER = "value.deserializer";

    /**
     * encode for deserializer String value
     */
    public static final String VALUE_DESERIALIZER_ENCODING = "value.deserializer.encoding";

    /**
     * connection ip
     */
    public static final String CONNECT_IP = "td.connect.ip";

    /**
     * connection port
     */
    public static final String CONNECT_PORT = "td.connect.port";

    /**
     * connection username
     */
    public static final String CONNECT_USER = "td.connect.user";

    /**
     * connection password
     */
    public static final String CONNECT_PASS = "td.connect.pass";

    /**
     * databaseName option
     */
    public static final String CONNECT_DB = "td.connect.db";

    /**
     * connect type websocket or jni, default is jni
     */
    public static final String CONNECT_TYPE = "td.connect.type";

    /**
     * Key used to retrieve the token value from the properties instance passed to
     * the driver.
     * Just for Cloud Service
     */
    public static final String CONNECT_TOKEN = TSDBDriver.PROPERTY_KEY_TOKEN;

    static {
        configSet.add(GROUP_ID);
        configSet.add(CLIENT_ID);
        configSet.add(ENABLE_AUTO_COMMIT);
        configSet.add(AUTO_COMMIT_INTERVAL);
        configSet.add(AUTO_OFFSET_RESET);
        configSet.add(MSG_WITH_TABLE_NAME);
        configSet.add(CONNECT_IP);
        configSet.add(CONNECT_USER);
        configSet.add(CONNECT_PASS);
        configSet.add(CONNECT_PORT);
        configSet.add(CONNECT_DB);
    }
}
