package com.taosdata.jdbc.tmq;

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

    /**
     * indicate which connection to create and username password...
     */
    public static final String CONNECT_URL = "td.connect.url";


    /*---- error code ----*/
    public static final int TMQ_SUCCESS = 0;
    public static final int TMQ_CONF_NULL = -100;
    public static final int TMQ_CONF_KEY_NULL = -101;
    public static final int TMQ_CONF_VALUE_NULL = -102;

    public static final int TMQ_TOPIC_NULL = -110;
    public static final int TMQ_TOPIC_NAME_NULL = -111;

    public static final int TMQ_CONSUMER_NULL = -120;
    public static final int TMQ_CONSUMER_CREATE_ERROR = -121;

    public static final int TMQ_UNKNOWN_ERROR = -999;
}
