package com.taosdata.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.JsonUtil;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.ws.*;
import com.taosdata.jdbc.ws.entity.*;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

public abstract class AbstractDriver implements Driver {
    private final org.slf4j.Logger log = LoggerFactory.getLogger(AbstractDriver.class);

    protected DriverPropertyInfo[] getPropertyInfo(Properties info) {
        DriverPropertyInfo hostProp = new DriverPropertyInfo(TSDBDriver.PROPERTY_KEY_HOST, info.getProperty(TSDBDriver.PROPERTY_KEY_HOST));
        hostProp.required = false;
        hostProp.description = "Hostname";

        DriverPropertyInfo portProp = new DriverPropertyInfo(TSDBDriver.PROPERTY_KEY_PORT, info.getProperty(TSDBDriver.PROPERTY_KEY_PORT));
        portProp.required = false;
        portProp.description = "Port";

        DriverPropertyInfo dbProp = new DriverPropertyInfo(TSDBDriver.PROPERTY_KEY_DBNAME, info.getProperty(TSDBDriver.PROPERTY_KEY_DBNAME));
        dbProp.required = false;
        dbProp.description = "Database name";

        DriverPropertyInfo userProp = new DriverPropertyInfo(TSDBDriver.PROPERTY_KEY_USER, info.getProperty(TSDBDriver.PROPERTY_KEY_USER));
        userProp.required = true;
        userProp.description = "User";

        DriverPropertyInfo passwordProp = new DriverPropertyInfo(TSDBDriver.PROPERTY_KEY_PASSWORD, info.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD));
        passwordProp.required = true;
        passwordProp.description = "Password";

        DriverPropertyInfo[] propertyInfo = new DriverPropertyInfo[5];
        propertyInfo[0] = hostProp;
        propertyInfo[1] = portProp;
        propertyInfo[2] = dbProp;
        propertyInfo[3] = userProp;
        propertyInfo[4] = passwordProp;
        return propertyInfo;
    }

    protected Properties parseURL(String url, Properties defaults) {
        return StringUtils.parseUrl(url, defaults);
    }



    protected Connection getWSConnection(String url, ConnectionParam param, Properties props) throws SQLException {
        if (log.isDebugEnabled()){
            log.debug("getWSConnection, url = {}", StringUtils.getBasicUrl(url));
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                log.debug("getWSConnection, ConnectionParam = {}", objectMapper.writeValueAsString(param));
            } catch (JsonProcessingException e) {
                log.error("Error serializing ConnectionParam", e);
            }
        }

        InFlightRequest inFlightRequest = new InFlightRequest(param.getRequestTimeout(), param.getMaxRequest());
        Transport transport = new Transport(WSFunction.WS, param, inFlightRequest);

        transport.setTextMessageHandler(message -> {
            try {
                JsonNode jsonObject = JsonUtil.getObjectReader().readTree(message);
                Action action = Action.of(jsonObject.get("action").asText());
                ObjectReader actionReader = JsonUtil.getObjectReader(action.getResponseClazz());
                Response response = actionReader.treeToValue(jsonObject, action.getResponseClazz());
                FutureResponse remove = inFlightRequest.remove(response.getAction(), response.getReqId());
                if (null != remove) {
                    remove.getFuture().complete(response);
                }
            } catch (JsonProcessingException e) {
                log.error("Error processing message", e);
            }
        });
        transport.setBinaryMessageHandler(byteBuffer -> {
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            byteBuffer.position(26);
            long id = byteBuffer.getLong();
            byteBuffer.position(8);
            FutureResponse remove = inFlightRequest.remove(Action.FETCH_BLOCK_NEW.getAction(), id);
            if (null != remove) {
                FetchBlockNewResp fetchBlockResp = new FetchBlockNewResp(byteBuffer);
                remove.getFuture().complete(fetchBlockResp);
            }
        });

        transport.checkConnection(param.getConnectTimeout());

        ConnectReq connectReq = new ConnectReq();
        connectReq.setReqId(1);
        connectReq.setUser(param.getUser());
        connectReq.setPassword(param.getPassword());
        connectReq.setDb(param.getDatabase());

        // Currently, only BI mode is supported. The downstream interface value is 0, so a conversion is performed here.
        if(param.getConnectMode() == ConnectionParam.CONNECT_MODE_BI){
            connectReq.setMode(0);
        }

        ConnectResp auth = (ConnectResp) transport.send(new Request(Action.CONN.getAction(), connectReq));

        if (Code.SUCCESS.getCode() != auth.getCode()) {
            transport.close();
            throw new SQLException("(0x" + Integer.toHexString(auth.getCode()) + "):" + "auth failure:" + auth.getMessage());
        }

        TaosGlobalConfig.setCharset(props.getProperty(TSDBDriver.PROPERTY_KEY_CHARSET));
        return new WSConnection(url, props, transport, param);
    }

}
