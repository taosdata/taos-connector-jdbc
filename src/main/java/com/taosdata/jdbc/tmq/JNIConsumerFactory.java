package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.common.Consumer;
import com.taosdata.jdbc.common.ConsumerFactory;
import com.taosdata.jdbc.common.ConsumerManager;

public class JNIConsumerFactory implements ConsumerFactory {

    private static final String CONNECTION_TYPE = "jni";

    static {
        ConsumerManager.register(new JNIConsumerFactory());
    }

    @Override
    public boolean acceptsType(String type) {
        return null == type || CONNECTION_TYPE.equalsIgnoreCase(type);
    }

    @Override
    public Consumer<?> getConsumer() {
        return new JNIConsumer<>();
    }
}
