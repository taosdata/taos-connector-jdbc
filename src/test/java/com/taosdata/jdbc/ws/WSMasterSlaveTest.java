package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.common.BaseTest;
import com.taosdata.jdbc.tmq.ResultBean;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.tmq.TaosConsumer;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Properties;

@RunWith(CatalogRunner.class)
@TestTarget(alias = "websocket query test", author = "yjshe", version = "2.0.38")
@FixMethodOrder
public class WSMasterSlaveTest extends BaseTest {
    private static final String hostB = "vm95";
    private static final int portB = 6041;

    @Description("consumer")
    @Test
    public void consumerException() {
        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, "root");
        properties.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, "127.0.0.1:6041");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, hostB);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, String.valueOf(portB));

        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_bean");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.ResultDeserializer");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");

        try {
            TaosConsumer<ResultBean> consumer = new TaosConsumer<>(properties);
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            return;
        }
        Assert.fail();
    }
}
