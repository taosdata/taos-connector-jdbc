package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.DriverType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

public interface TAOSConsumer extends AutoCloseable {

    void subscribe(Collection<String> topics) throws SQLException;

    void unsubscribe() throws SQLException;

    Set<String> subscription() throws SQLException;

    ResultSet poll(Duration timeout);

    void commitAsync();

    void commitAsync(Consumer<CallbackResult> consumer);

    void commitSync() throws SQLException;

    String getTopicName();

    String getDatabaseName();

    int getVgroupId();

    String getTableName();

    @Override
    void close() throws SQLException;

    static TAOSConsumer getInstance(Properties properties, Consumer<CallbackResult> consumer) throws SQLException {
        if (null == properties) {
            return new JNIConsumer(null, consumer);
        }
        String url = properties.getProperty(TMQConstants.CONNECT_URL);
        if (url == null) {
            return new JNIConsumer(properties, consumer);
        }
        if (DriverType.JNI == DriverType.getType(url)) {
            return new JNIConsumer(properties, consumer);
        } else {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "In TMQ connection：" + url + " is not supported currently");
        }
    }

    static TAOSConsumer getInstance(Properties properties) throws SQLException {
        return getInstance(properties, null);
    }
}
