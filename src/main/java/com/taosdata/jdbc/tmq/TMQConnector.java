package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.*;
import com.taosdata.jdbc.utils.StringUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static com.taosdata.jdbc.tmq.TMQConstants.*;

public class TMQConnector extends TSDBJNIConnector {

    private String createConsumerErrorMsg;
    private String[] topics;

    public long createConfig(Properties properties, JNIConsumer consumer) throws SQLException {
        long conf = tmqConfNewImp(consumer);
        if (null == properties || properties.size() < 1)
            return conf;
        for (Map.Entry<?, ?> entry : properties.entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (!StringUtils.isEmpty(key) && TMQConstants.configSet.contains(key)) {
                int code = tmqConfSetImp(conf, key, String.valueOf(entry.getValue()));
                if (code == TMQConstants.TMQ_CONF_KEY_NULL) {
                    tmqConfDestroyImp(conf);
                    throw TSDBError.createSQLException(code, "Failed to set tmq property. key is null");
                }
                if (code == TMQConstants.TMQ_CONF_VALUE_NULL) {
                    tmqConfDestroyImp(conf);
                    throw TSDBError.createSQLException(code, "Failed to set tmq property : " + key + ". value is null");
                }
                if (code < 0) {
                    tmqConfDestroyImp(conf);
                    throw TSDBError.createSQLException(code,
                            "Failed to set consumer config property : " + key + ". reason: " + getErrMsg(code));
                }
            }
        }
        return conf;
    }

    // DLL_EXPORT tmq_conf_t *tmq_conf_new();
    private native long tmqConfNewImp(JNIConsumer consumer);

    // DLL_EXPORT tmq_conf_res_t tmq_conf_set(tmq_conf_t *conf,
    // const char *key, const char *value);
    private native int tmqConfSetImp(long conf, String key, String value);

    public void destroyConf(long conf) {
        tmqConfDestroyImp(conf);
    }

    // DLL_EXPORT void tmq_conf_destroy(tmq_conf_t *conf);
    private native void tmqConfDestroyImp(long conf);

    public void createConsumer(long conf) throws SQLException {
        taos = tmqConsumerNewImp(conf, this);
        if (taos == TMQ_CONF_NULL) {
            throw TSDBError.createSQLException(TMQ_CONF_NULL, "consumer config reference has been destroyed");
        }
        if (taos < 0) {
            throw TSDBError.createSQLException(TMQConstants.TMQ_CONSUMER_CREATE_ERROR, createConsumerErrorMsg);
        }
    }

    // DLL_EXPORT tmq_t *tmq_consumer_new(tmq_conf_t *conf,
    // char *errstr, int32_t errstrLen);
    private native long tmqConsumerNewImp(long conf, TMQConnector connector);

    void setCreateConsumerErrorMsg(String msg) {
        this.createConsumerErrorMsg = msg;
    }

    public long createTopic(Collection<String> topics) throws SQLException {
        long topic = tmqTopicNewImp(taos);
        if (topic < TMQ_SUCCESS) {
            throw TSDBError.createSQLException(TMQ_CONSUMER_NULL, "Failed to create tmq topic, consumer reference is null");
        }
        if (null != topics && topics.size() > 0) {
            for (String name : topics) {
                int code = tmqTopicAppendImp(topic, name);
                if (code == TMQ_TOPIC_NULL) {
                    destroyTopic(topic);
                    throw TSDBError.createSQLException(TMQ_TOPIC_NULL, "Failed to set consumer topic");
                }
                if (code == TMQ_TOPIC_NAME_NULL) {
                    destroyTopic(topic);
                    throw TSDBError.createSQLException(TMQ_TOPIC_NAME_NULL,
                            "Failed to set consumer topic, topic name is empty");
                }
                if (code != TMQ_SUCCESS) {
                    destroyTopic(topic);
                    throw TSDBError.createSQLException(TMQ_UNKNOWN_ERROR, "Failed to set consumer topic.");
                }
            }
        }
        return topic;
    }

    // DLL_EXPORT tmq_list_t *tmq_list_new();
    private native long tmqTopicNewImp(long tmq);

    // DLL_EXPORT int32_t tmq_list_append(tmq_list_t *, const char *);
    private native int tmqTopicAppendImp(long topic, String topicName);

    public void destroyTopic(long topic) {
        tmqTopicDestroyImp(topic);
    }

    // DLL_EXPORT void tmq_list_destroy(tmq_list_t *);
    private native void tmqTopicDestroyImp(long topic);

    public void subscribe(long topic) throws SQLException {
        int code = tmqSubscribeImp(taos, topic);
        if (code == TMQ_CONSUMER_NULL) {
            throw TSDBError.createSQLException(TMQ_CONSUMER_NULL, "consumer reference has been destroyed");
        }
        if (code == TMQ_TOPIC_NULL) {
            throw TSDBError.createSQLException(TMQ_TOPIC_NULL, "topic reference has been destroyed");
        }
        if (code != TMQ_SUCCESS) {
            throw TSDBError.createSQLException(code, getErrMsg(code));
        }
    }

    // DLL_EXPORT tmq_resp_err_t tmq_subscribe(tmq_t *tmq,
    // const tmq_list_t *topic_list);
    private native int tmqSubscribeImp(long tmq, long topic);

    public Set<String> subscription() throws SQLException {
        int code = tmqSubscriptionImp(taos, this);
        if (code == TMQ_CONSUMER_NULL) {
            throw TSDBError.createSQLException(TMQ_CONSUMER_NULL, "consumer reference has been destroyed");
        }
        if (code != TMQ_SUCCESS) {
            throw TSDBError.createSQLException(code, getErrMsg(code));
        }
        return Arrays.stream(topics).collect(Collectors.toSet());
    }

    // DLL_EXPORT tmq_resp_err_t tmq_subscription(tmq_t *tmq, tmq_list_t **topics);
    private native int tmqSubscriptionImp(long tmq, TMQConnector connector);

    public void setTopicList(String[] topics) {
        this.topics = topics;
    }

    public void syncCommit(long offsets) throws SQLException {
        int code = tmqCommitSync(taos, offsets);
        if (code == TMQ_CONSUMER_NULL) {
            throw TSDBError.createSQLException(TMQ_CONSUMER_NULL, "consumer reference has been destroyed");
        }
        if (code != TMQ_SUCCESS) {
            throw TSDBError.createSQLException(code, createConsumerErrorMsg);
        }
    }

    // DLL_EXPORT tmq_resp_err_t tmq_commit_sync(tmq_t *tmq,
    // const tmq_topic_vgroup_list_t *offsets);
    private native int tmqCommitSync(long tmq, long offsets);

    public void asyncCommit(long offsets, JNIConsumer consumer) {
        tmqCommitAsync(taos, offsets, consumer);
    }

    // DLL_EXPORT void tmq_commit_async(tmq_t *tmq,
    // const tmq_topic_vgroup_list_t *offsets, tmq_commit_cb *cb, void *param);
    private native void tmqCommitAsync(long tmq, long offsets, JNIConsumer consumer);

    public void unsubscribe() throws SQLException {
        int code = tmqUnsubscribeImp(taos);
        if (code == TMQ_CONSUMER_NULL) {
            throw TSDBError.createSQLException(TMQ_CONSUMER_NULL, "consumer reference has been destroyed");
        }
        if (code != TMQ_SUCCESS) {
            throw TSDBError.createSQLException(code, getErrMsg(code));
        }
    }

    // DLL_EXPORT tmq_resp_err_t tmq_unsubscribe(tmq_t *tmq);
    private native int tmqUnsubscribeImp(long tmq);

    public void closeConsumer() throws SQLException {
        int code = tmqConsumerCloseImp(taos);
        if (code != TMQ_SUCCESS && code != TMQ_CONSUMER_NULL) {
            throw TSDBError.createSQLException(code, getErrMsgImp(code));
        }
    }

    // DLL_EXPORT tmq_resp_err_t tmq_consumer_close(tmq_t *tmq);
    private native int tmqConsumerCloseImp(long tmq);


    // TODO confirm code range, which is applied to
    public String getErrMsg(int code) {
        return this.getErrMsgImp(code);
    }

    // DLL_EXPORT const char *tmq_err2str(tmq_resp_err_t);
    private native String getErrMsgImp(int code);

    public long poll(long waitTime) throws SQLException {
        long res = tmqConsumerPoll(taos, waitTime);
        if (res == TMQ_CONSUMER_NULL) {
            throw TSDBError.createSQLException(TMQ_CONSUMER_NULL, "consumer reference has been destroyed");
        }
        return res;
    }

    // DLL_EXPORT TAOS_RES *tmq_consumer_poll(tmq_t *tmq, int64_t wait_time);
    private native long tmqConsumerPoll(long tmq, long waitTime);


    public String getTopicName(long res) {
        return tmqGetTableName(res);
    }

    // DLL_EXPORT const char *tmq_get_topic_name(TAOS_RES *res);
    private native String tmqGetTopicName(long res);

    public String getDbName(long res) {
        return tmqGetDbName(res);
    }

    // DLL_EXPORT const char *tmq_get_db_name(TAOS_RES *res);
    private native String tmqGetDbName(long res);

    public int getVgroupId(long res) {
        return tmqGetVgroupId(res);
    }

    // DLL_EXPORT int32_t tmq_get_vgroup_id(TAOS_RES *res);
    private native int tmqGetVgroupId(long res);

    public String getTableName(long res) {
        return tmqGetTableName(res);
    }

    // DLL_EXPORT const char *tmq_get_table_name(TAOS_RES *res);
    private native String tmqGetTableName(long res);
}
