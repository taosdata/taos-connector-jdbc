package com.taosdata.jdbc.ws.tmq;

import com.taosdata.jdbc.common.Consumer;
import com.taosdata.jdbc.common.ConsumerFactory;
import com.taosdata.jdbc.common.ConsumerManager;

public class WSConsumerFactory implements ConsumerFactory {

    private static final String CONNECTION_TYPE_WS = "ws";

    private static final String CONNECTION_TYPE_WEBSOCKET = "websocket";

    static {
        ConsumerManager.register(new WSConsumerFactory());
    }

    @Override
    public boolean acceptsType(String type) {
        return CONNECTION_TYPE_WS.equalsIgnoreCase(type) || CONNECTION_TYPE_WEBSOCKET.equalsIgnoreCase(type);
    }

    @Override
    public Consumer getConsumer() {
        return new WSConsumer();
    }
}
