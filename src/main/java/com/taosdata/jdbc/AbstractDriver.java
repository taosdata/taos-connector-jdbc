package com.taosdata.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.rs.ConnectionParam;
import com.taosdata.jdbc.utils.*;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.InFlightRequest;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.WSConnection;
import com.taosdata.jdbc.ws.entity.*;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import static com.taosdata.jdbc.TSDBConstants.MIN_SUPPORT_VERSION;

public abstract class AbstractDriver implements Driver {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(AbstractDriver.class);

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
    protected Connection getWSConnection(String url, ConnectionParam param, Properties props) throws SQLException {
        if (log.isDebugEnabled()){
            log.debug("getWSConnection, url = {}", StringUtils.getBasicUrl(url));
            try {
                ObjectMapper objectMapper = JsonUtil.getObjectMapper();
                log.debug("getWSConnection, ConnectionParam = {}", objectMapper.writeValueAsString(param));
            } catch (JsonProcessingException e) {
                log.error("Error serializing ConnectionParam", e);
            }
        }
        InFlightRequest inFlightRequest = new InFlightRequest(param.getRequestTimeout(), param.getMaxRequest());
        param.setTextMessageHandler(message -> {
            try {
                log.trace("received message: {}", message);
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

        param.setBinaryMessageHandler(byteBuf -> {
            byteBuf.readerIndex(26);
            long id = byteBuf.readLongLE();
            byteBuf.readerIndex(8);

            FutureResponse remove = inFlightRequest.remove(Action.FETCH_BLOCK_NEW.getAction(), id);
            if (null != remove) {
                Utils.retainByteBuf(byteBuf);
                FetchBlockNewResp fetchBlockResp = new FetchBlockNewResp(byteBuf);
                remove.getFuture().complete(fetchBlockResp);
            }
        });
        Transport transport = new Transport(WSFunction.WS, param, inFlightRequest);

        transport.checkConnection(param.getConnectTimeout());

        ConnectReq connectReq = new ConnectReq(param);
        ConnectResp auth = (ConnectResp) transport.send(new Request(Action.CONN.getAction(), connectReq));

        if (Code.SUCCESS.getCode() != auth.getCode()) {
            transport.close();
            throw new SQLException("(0x" + Integer.toHexString(auth.getCode()) + "):" + "auth failure:" + auth.getMessage());
        }

        String version = auth.getVersion();
        if (version == null){
            version = VersionUtil.getVersion(transport);
        }

        if (!VersionUtil.checkVersion(version)) {
            transport.close();
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_VERSION_INCOPMATIABLE, "minimal supported version is " + MIN_SUPPORT_VERSION + ", but got " + version);
        }

        TaosGlobalConfig.setCharset(StandardCharsets.UTF_8.name());
        return new WSConnection(url, props, transport, param, version);
    }

}
