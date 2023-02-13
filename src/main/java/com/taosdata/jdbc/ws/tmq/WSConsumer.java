package com.taosdata.jdbc.ws.tmq;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.Consumer;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.InFlightRequest;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.FetchBlockResp;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.entity.Response;
import com.taosdata.jdbc.ws.tmq.entity.ConsumerParam;
import com.taosdata.jdbc.ws.tmq.entity.PollResp;
import com.taosdata.jdbc.ws.tmq.entity.SubscribeResp;
import com.taosdata.jdbc.ws.tmq.entity.TMQRequestFactory;

import java.nio.ByteOrder;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;

public class WSConsumer<V> implements Consumer<V> {
    private Transport transport;
    private ConsumerParam param;
    private TMQRequestFactory factory;
    private List<V> list = new ArrayList<>();

    @Override
    public void create(Properties properties) throws SQLException {
        factory = new TMQRequestFactory();
        param = new ConsumerParam(properties);
        InFlightRequest inFlightRequest = new InFlightRequest(param.getConnectionParam().getRequestTimeout()
                , param.getConnectionParam().getMaxRequest());
        transport = new Transport(WSFunction.TMQ, param.getConnectionParam(), inFlightRequest);

        transport.setTextMessageHandler(message -> {
            JSONObject jsonObject = JSON.parseObject(message);
            TMQAction action = TMQAction.of(jsonObject.getString("action"));
            Response response = jsonObject.toJavaObject(action.getResponseClazz());
            FutureResponse remove = inFlightRequest.remove(response.getAction(), response.getReqId());
            if (null != remove) {
                remove.getFuture().complete(response);
            }
        });
        transport.setBinaryMessageHandler(byteBuffer -> {
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            byteBuffer.position(8);
            // request_id
            long id = byteBuffer.getLong();
            byteBuffer.position(24);
            FutureResponse remove = inFlightRequest.remove(TMQAction.FETCH_BLOCK.getAction(), id);
            if (null != remove) {
                FetchBlockResp fetchBlockResp = new FetchBlockResp(id, byteBuffer);
                remove.getFuture().complete(fetchBlockResp);
            }
        });

        Transport.checkConnection(transport, param.getConnectionParam().getConnectTimeout());
    }

    @Override
    public void subscribe(Collection<String> topics) throws SQLException {
        Request request = factory.generateSubscribe(param.getConnectionParam().getUser()
                , param.getConnectionParam().getPassword()
                , param.getConnectionParam().getDatabase()
                , param.getGroupId()
                , param.getClientId()
                , param.getOffsetRest()
                , topics.toArray(new String[0]));
        SubscribeResp response = (SubscribeResp) transport.send(request);
        if (Code.SUCCESS.getCode() != response.getCode()) {
            throw new SQLException("subscribe topic error: " + response.getMessage());
        }
    }

    @Override
    public void unsubscribe() throws SQLException {
        // nothing to do
    }

    @Override
    public Set<String> subscription() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public ConsumerRecords<V> poll(Duration timeout, Deserializer<V> deserializer) throws SQLException {
        Request request = factory.generatePoll(timeout.toMillis());
        PollResp pollResp = (PollResp) transport.send(request);
        if (Code.SUCCESS.getCode() != pollResp.getCode()) {
            throw new SQLException("consumer poll error: " + pollResp.getMessage());
        }
        if (!pollResp.isHaveMessage()) {
            return ConsumerRecords.empty();
        }
        String topic = pollResp.getTopic();
        String dbName = pollResp.getDatabase();
        int vGroupId = pollResp.getVgroupId();
        TopicPartition tmp = new TopicPartition(topic, dbName, vGroupId);

        Map<TopicPartition, List<V>> records = new HashMap<>();
        TopicPartition partition = null;
        try (WSTMQResultSet rs = new WSTMQResultSet(transport, factory, pollResp.getMessageId(), dbName)) {
            while (rs.next()) {
                if (!tmp.equals(partition)) {
                    records.put(partition, list);
                    partition = tmp;
                    list = new ArrayList<>();
                }
                try {
                    V record = deserializer.deserialize(rs);
                    list.add(record);
                } catch (Exception e) {
                    throw new DeserializerException("Deserializer error", e);
                }
            }
        }
        records.put(partition, list);
        return new ConsumerRecords<>(records);
    }

    @Override
    public void commitSync() throws SQLException {

    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public void commitCallbackHandler(int code, OffsetCommitCallback callback) {

    }

    @Override
    public void commitAsync(TaosConsumer<?> consumer) {

    }


}
