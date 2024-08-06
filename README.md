# TDengine Java Connector

English | [简体中文](./README-CN.md)

'taos-jdbcdriver' is TDengine's official Java language connector, which allows Java developers to develop applications that access the TDengine database. 'taos-jdbcdriver' implements the interface of the JDBC driver standard and provides two forms of connectors. One is to connect to a TDengine instance natively through the TDengine client driver (taosc), which supports functions including data writing, querying, subscription, schemaless writing, and bind interface. And the other is to connect to a TDengine instance through the Websocket interface provided by taosAdapter (2.4.0.0 and later). Websocket connections implement has a slight differences to compare the set of features implemented and native connections.

![tdengine-connector](tdengine-jdbc-connector.png)

The preceding diagram shows two ways for a Java app to access TDengine via connector:

- JDBC native connection: Java applications use TSDBDriver on physical node 1 (pnode1) to call client-driven directly (`libtaos.so` or `taos.dll`) APIs to send writing and query requests to taosd instances located on physical node 2 (pnode2).
- JDBC Websocket connection: The Java application encapsulates the SQL as a Websocket request via RestfulDriver, sends it to the Websocket server of physical node 2 (taosAdapter), requests TDengine server through the Websocket server, and returns the result.

Using Websocket connection, which does not rely on TDengine client drivers.It can be cross-platform more convenient and flexible, and the performance is close to native connection.

**Note**:

TDengine's JDBC driver implementation is as consistent as possible with the relational database driver. Still, there are differences in the use scenarios and technical characteristics of TDengine and relational object databases, so 'taos-jdbcdriver' also has some differences from traditional JDBC drivers. You need to pay attention to the following points when using:

- TDengine does not currently support delete operations for individual data records.
- Transactional operations are not currently supported.

## Supported platforms

Native connection supports the same platform as TDengine client-driven support.
Websocket connection supports all platforms that can run Java.

## Version support

Please refer to [Version Support List](https://docs.tdengine.com/client-libraries/#version-support).

## TDengine DataType vs. Java DataType

TDengine currently supports timestamp, number, character, Boolean type, and the corresponding type conversion with Java is as follows:


| TDengine DataType | JDBCType           |
| ----------------- | ------------------ |
| TIMESTAMP         | java.sql.Timestamp |
| INT               | java.lang.Integer  |
| BIGINT            | java.lang.Long     |
| FLOAT             | java.lang.Float    |
| DOUBLE            | java.lang.Double   |
| SMALLINT          | java.lang.Short    |
| TINYINT           | java.lang.Byte     |
| BOOL              | java.lang.Boolean  |
| BINARY            | byte array         |
| NCHAR             | java.lang.String   |
| JSON              | java.lang.String   |
| VARBINARY         | byte[]             |
| GEOMETRY          | byte[]             |


**Note**: Only TAG supports JSON types

## Installation steps

### Pre-installation preparation

Before using Java Connector to connect to the database, the following conditions are required.

- Java 1.8 or above runtime environment and Maven 3.6 or above installed
- TDengine client driver installed (required for native connections, not required for Websocket connections), please refer to [Installing Client Driver](https://docs.tdengine.com/client-libraries/#install-client-driver)

## Install the connectors

### Build with Maven

- [sonatype](https://search.maven.org/artifact/com.taosdata.jdbc/taos-jdbcdriver)
- [mvnrepository](https://mvnrepository.com/artifact/com.taosdata.jdbc/taos-jdbcdriver)
- [maven.aliyun](https://maven.aliyun.com/mvn/search)

Add following dependency in the `pom.xml` file of your Maven project:

```xml-dtd
<dependency>
 <groupId>com.taosdata.jdbc</groupId>
 <artifactId>taos-jdbcdriver</artifactId>
 <version>3.0.*</version>
</dependency>
```

### Build with source code

You can build Java connector from source code after clone TDengine project:

```shell
git clone https://github.com/taosdata/taos-connector-jdbc.git
cd taos-connector-jdbc
mvn clean install -Dmaven.test.skip=true
```

After compilation, a jar package of taos-jdbcdriver-3.0.0-dist .jar is generated in the target directory, and the compiled jar file is automatically placed in the local Maven repository.

## Establish a connection

TDengine's JDBC URL specification format is:
`jdbc:[TAOS| TAOS-RS]://[host_name]:[port]/[database_name]? [user={user}|&password={password}|&charset={charset}|&cfgdir={config_dir}|&locale={locale}|&timezone={timezone}]`

For establishing connections, native connections differ slightly from REST connections.

### Native connection

```java
Class.forName("com.taosdata.jdbc.TSDBDriver");
String jdbcUrl = "jdbc:TAOS://taosdemo.com:6030/test?user=root&password=taosdata";
Connection conn = DriverManager.getConnection(jdbcUrl);
```

In the above example, TSDBDriver, which uses a JDBC native connection, establishes a connection to a hostname `taosdemo.com`, port `6030` (the default port for TDengine), and a database named `test`. In this URL, the user name `user` is specified as `root`, and the `password` is `taosdata`.

**Note**: With JDBC native connections, taos-jdbcdriver relies on the client driver (`libtaos.so` on Linux; `taos.dll` on Windows).

The configuration parameters in the URL are as follows:

- user: Log in to the TDengine username. The default value is 'root'.
- password: User login password, the default value is 'taosdata'.
- cfgdir: client configuration file directory path, default '/etc/taos' on Linux OS, 'C:/TDengine/cfg' on Windows OS.
- charset: The character set used by the client, the default value is the system character set.
- locale: Client locale, by default, use the system's current locale.
- timezone: The time zone used by the client, the default value is the system's current time zone.
- batchfetch: true: pulls result sets in batches when executing queries; false: pulls result sets row by row. The default value is: true. Enabling batch pulling and obtaining a batch of data can improve query performance when the query data volume is large.
- batchErrorIgnore:true: When executing statement executeBatch, if there is a SQL execution failure in the middle, the following SQL will continue to be executed. false: No more statements after the failed SQL are executed. The default value is: false.

For more information about JDBC native connections, see [Video Tutorial](https://www.taosdata.com/blog/2020/11/11/1955.html).

**Connect using the TDengine client-driven configuration file**

When you use a JDBC native connection to connect to a TDengine cluster, you can use the TDengine client driver configuration file to specify parameters such as `firstEp` and `secondEp` of the cluster in the configuration file as below:

1. Do not specify hostname and port in Java applications.

```java
public Connection getConn() throws Exception{
  Class.forName("com.taosdata.jdbc.TSDBDriver");
  String jdbcUrl = "jdbc:TAOS://:/test?user=root&password=taosdata";
  Properties connProps = new Properties();
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
  Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
  return conn;
}
```

2. specify the firstEp and the secondEp in the configuration file taos.cfg

```shell
# first fully qualified domain name (FQDN) for TDengine system
firstEp               cluster_node1:6030

# second fully qualified domain name (FQDN) for TDengine system, for cluster only
secondEp              cluster_node2:6030

# default system charset
# charset               UTF-8

# system locale
# locale                en_US.UTF-8
```

In the above example, JDBC uses the client's configuration file to establish a connection to a hostname `cluster_node1`, port 6030, and a database named `test`. When the firstEp node in the cluster fails, JDBC attempts to connect to the cluster using secondEp.

In TDengine, as long as one node in firstEp and secondEp is valid, the connection to the cluster can be established normally.

> **Note**: The configuration file here refers to the configuration file on the machine where the application that calls the JDBC Connector is located, the default path is `/etc/taos/taos.cfg` on Linux, and the default path is `C://TDengine/cfg/taos.cfg` on Windows.

### REST connection

```java
Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
String jdbcUrl = "jdbc:TAOS-RS://taosdemo.com:6041/test?user=root&password=taosdata";
Connection conn = DriverManager.getConnection(jdbcUrl);
```

In the above example, a RestfulDriver with a JDBC REST connection is used to establish a connection to a database named `test` with hostname `taosdemo.com` on port `6041`. The URL specifies the user name as `root` and the password as `taosdata`.

There is no dependency on the client driver when Using a JDBC Websocket connection. Compared to a JDBC native connection, only the following are required: 1.

1. driverClass specified as "com.taosdata.jdbc.rs.RestfulDriver".
2. jdbcUrl starting with "jdbc:TAOS-RS://".
3. use 6041 as the connection port.
4. add url parameter `batchfetch` with true.

The configuration parameters in the URL are as follows.

- user: Login TDengine user name, default value 'root'.
- password: user login password, default value 'taosdata'.
- batchfetch: true: pull the result set in batch when executing the query; false: pull the result set row by row. The default value is false. batchfetch uses HTTP for data transfer. The JDBC REST connection supports bulk data pulling function. taos-jdbcdriver and TDengine transfer data via WebSocket connection. Compared with HTTP, WebSocket enables JDBC REST connection to support large data volume querying and improve query performance.
- charset: specify the charset to parse the string, this parameter is valid only when set batchfetch to true.
- batchErrorIgnore: true: when executing executeBatch of Statement, if one SQL execution fails in the middle, continue to execute the following SQL. false: no longer execute any statement after the failed SQL. The default value is: false.
- httpConnectTimeout: Websocket connection timeout in milliseconds, the default value is 5000 ms.
- messageWaitTimeout: message transmission timeout in milliseconds, the default value is 3000 ms. 
- useSSL: connecting Securely Using SSL. true: using SSL connection, false: not using SSL connection.

**Note**: Some configuration items (e.g., locale, timezone) do not work in the Websocket connection.


### Specify the URL and Properties to get the connection

In addition to getting the connection from the specified URL, you can use Properties to specify parameters when the connection is established.

**Note**:

- The client parameter set in the application is process-level. If you want to update the parameters of the client, you need to restart the application. This is because the client parameter is a global parameter that takes effect only the first time the application is set.
- The following sample code is based on taos-jdbcdriver-3.0.0.

```java
public Connection getConn() throws Exception{
  Class.forName("com.taosdata.jdbc.TSDBDriver");
  String jdbcUrl = "jdbc:TAOS://taosdemo.com:6030/test?user=root&password=taosdata";
  Properties connProps = new Properties();
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
  connProps.setProperty("debugFlag", "135");
  connProps.setProperty("maxSQLLength", "1048576");
  Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
  return conn;
}

public Connection getRestConn() throws Exception{
  Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
  String jdbcUrl = "jdbc:TAOS-RS://taosdemo.com:6041/test?user=root&password=taosdata";
  Properties connProps = new Properties();
  connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
  Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
  return conn;
}
```

In the above example, a connection is established to `taosdemo.com`, port is 6030/6041, and database named `test`. The connection specifies the user name as `root` and the password as `taosdata` in the URL and specifies the character set, language environment, time zone, and whether to enable bulk fetching in the connProps.

The configuration parameters in properties are as follows.

- TSDBDriver.PROPERTY_KEY_USER: Login TDengine user name, default value 'root'.
- TSDBDriver.PROPERTY_KEY_PASSWORD: user login password, default value 'taosdata'.
- TSDBDriver.PROPERTY_KEY_BATCH_LOAD: true: pull the result set in batch when executing query; false: pull the result set row by row. The default value is: false.
- TSDBDriver.PROPERTY_KEY_BATCH_ERROR_IGNORE: true: when executing executeBatch of Statement, if there is a SQL execution failure in the middle, continue to execute the following sq. false: no longer execute any statement after the failed SQL. The default value is: false.
- TSDBDriver.PROPERTY_KEY_CONFIG_DIR: Only works when using JDBC native connection. Client configuration file directory path, default value `/etc/taos` on Linux OS, default value `C:/TDengine/cfg` on Windows OS.
- TSDBDriver.PROPERTY_KEY_CHARSET: The character set used by the client, the default value is the system setting.
- TSDBDriver.PROPERTY_KEY_LOCALE: this only takes effect when using JDBC native connection. Client language environment, the default value is system current locale.
- TSDBDriver.PROPERTY_KEY_TIME_ZONE: only takes effect when using JDBC native connection. In the time zone used by the client, the default value is the system's current time zone.
- TSDBDriver.HTTP_CONNECT_TIMEOUT: Websocket connection timeout in milliseconds, the default value is 5000 ms. It only takes effect when using JDBC Websocket connection.
- TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT: message transmission timeout in milliseconds, the default value is 3000 ms. It only takes effect when using JDBC Websocket connection.
- TSDBDriver.PROPERTY_KEY_USE_SSL: connecting Securely Using SSL. true: using SSL connection, false: not using SSL connection. It only takes effect when using using JDBC Websocket connection.
  For JDBC native connections, you can specify other parameters, such as log level, SQL length, etc., by specifying URL and Properties. For more detailed configuration, please refer to [Client Configuration](https://docs.taosdata.com/reference/config/#Client-Only).

### Priority of configuration parameters

If the configuration parameters are duplicated in the URL, Properties, or client configuration file, the `priority` of the parameters, from highest to lowest, are as follows:

1. JDBC URL parameters, as described above, can be specified in the parameters of the JDBC URL.
2. Properties connProps
3. the configuration file taos.cfg of the TDengine client driver when using a native connection

For example, if you specify the password as `taosdata` in the URL and specify the password as `taosdemo` in the Properties simultaneously. In this case, JDBC will use the password in the URL to establish the connection.

## Usage examples
All code examples use WebSocket connections. If you want to use a native connection, you usually only need to modify the JDBC URL.

### Create database and tables

```java
try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
     Statement stmt = connection.createStatement()) {

    // create database
    int rowsAffected = stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS power");
    // you can check rowsAffected here
    assert rowsAffected == 0;

    // use database
    rowsAffected = stmt.executeUpdate("USE power");
    // you can check rowsAffected here
    assert rowsAffected == 0;

    // create table
    rowsAffected = stmt.executeUpdate("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
    // you can check rowsAffected here
    assert rowsAffected == 0;

} catch (SQLException ex) {
    // handle any errors, please refer to the JDBC specifications for detailed exceptions info
    System.out.println("Failed to create db and table, url:" + jdbcUrl + "; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
    throw ex;
} catch (Exception ex){
    System.out.println("Failed to create db and table, url:" + jdbcUrl + "; ErrMessage: " + ex.getMessage());
    throw ex;
}
```

> **Note**: If you do not use `use db` to specify the database, all subsequent operations on the table need to add the database name as a prefix, such as db.tb.

### Insert data

```java
// insert data
try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
     Statement stmt = connection.createStatement()) {

    // insert data, please make sure the database and table are created before
    String insertQuery = "INSERT INTO " +
            "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
            "VALUES " +
            "(NOW + 1a, 10.30000, 219, 0.31000) " +
            "(NOW + 2a, 12.60000, 218, 0.33000) " +
            "(NOW + 3a, 12.30000, 221, 0.31000) " +
            "power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') " +
            "VALUES " +
            "(NOW + 1a, 10.30000, 218, 0.25000) ";
    int affectedRows = stmt.executeUpdate(insertQuery);
    // you can check affectedRows here
    System.out.println("inserted into " + affectedRows + " rows to power.meters successfully.");
} catch (SQLException ex) {
    // handle any errors, please refer to the JDBC specifications for detailed exceptions info
    System.out.println("Failed to insert data to power.meters, url:" + jdbcUrl + "; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
    throw ex;
} catch (Exception ex){
    System.out.println("Failed to insert data to power.meters, url:" + jdbcUrl + "; ErrMessage: " + ex.getMessage());
    throw ex;
}
```


> now is an internal function. The default is the current time of the client's computer.
> `now + 1s` represents the current time of the client plus 1 second, followed by the number representing the unit of time: a (milliseconds), s (seconds), m (minutes), h (hours), d (days), w (weeks), n (months), y (years).

### Querying data

```java
try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
     Statement stmt = connection.createStatement();
     // query data, make sure the database and table are created before
     ResultSet resultSet = stmt.executeQuery("SELECT ts, current, location FROM power.meters limit 100")) {

    Timestamp ts;
    float current;
    String location;
    while (resultSet.next()) {
        ts = resultSet.getTimestamp(1);
        current = resultSet.getFloat(2);
        // we recommend using the column name to get the value
        location = resultSet.getString("location");

        // you can check data here
        System.out.printf("ts: %s, current: %f, location: %s %n", ts, current, location);
    }
} catch (SQLException ex) {
    // handle any errors, please refer to the JDBC specifications for detailed exceptions info
    System.out.println("Failed to query data from power.meters, url:" + jdbcUrl + "; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
    throw ex;
} catch (Exception ex){
    System.out.println("Failed to query data from power.meters, url:" + jdbcUrl + "; ErrMessage: " + ex.getMessage());
    throw ex;
}
```

> The query is consistent with operating a relational database. When using subscripts to get the contents of the returned fields, starting from 1, it is recommended to use the field names to get them.

### Handling exceptions

After an error is reported, the error message and error code can be obtained through SQLException.

```java
try (Statement statement = connection.createStatement();
     // executeQuery
     ResultSet tempResultSet = statement.executeQuery(sql)) {

    // print result
    printResult(tempResultSet);
} catch (SQLException ex) {
    System.out.println("ERROR Message: " + ex.getMessage() + "ERROR Code: " + ex.getErrorCode());
    ex.printStackTrace();
} catch (Exception ex){
    System.out.println("ERROR Message: " + ex.getMessage());
    ex.printStackTrace();
}
```

There are three types of error codes that the JDBC connector can report:

- Error code of the JDBC driver itself (error code between 0x2301 and 0x2350)
- Error code of the native connection method (error code between 0x2351 and 0x2400)
- Error code of other TDengine function modules

For specific error codes, please refer to.

- [TDengine Java Connector](https://github.com/taosdata/taos-connector-jdbc/blob/main/src/main/java/com/taosdata/jdbc/TSDBErrorNumbers.java)
- [TDengine_ERROR_CODE](https://github.com/taosdata/TDengine/blob/main/include/util/taoserror.h)

### Writing data via parameter binding

TDengine's native JDBC connection implementation has significantly improved its support for data writing (INSERT) scenarios via bind interface. Writing data in this way avoids the resource consumption of SQL syntax parsing, resulting in significant write performance improvements in many cases.

**Note**.

- The following sample code is based on taos-jdbcdriver-3.0.0
- The setString method should be called for binary type data, and the setNString method should be called for nchar type data
- both setString and setNString require the user to declare the width of the corresponding column in the size parameter of the table definition

```java
public class WSParameterBindingBasicDemo {

    // modify host to your own
    private static final String host = "127.0.0.1";
    private static final Random random = new Random(System.currentTimeMillis());
    private static final int numOfSubTable = 10, numOfRow = 10;

    public static void main(String[] args) throws SQLException {

        String jdbcUrl = "jdbc:TAOS-RS://" + host + ":6041/?batchfetch=true";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "root", "taosdata")) {
            init(conn);

            String sql = "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)";

            try (TSWSPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

                for (int i = 1; i <= numOfSubTable; i++) {
                    // set table name
                    pstmt.setTableName("d_bind_" + i);

                    // set tags
                    pstmt.setTagInt(0, i);
                    pstmt.setTagString(1, "location_" + i);

                    // set columns
                    long current = System.currentTimeMillis();
                    for (int j = 0; j < numOfRow; j++) {
                        pstmt.setTimestamp(1, new Timestamp(current + j));
                        pstmt.setFloat(2, random.nextFloat() * 30);
                        pstmt.setInt(3, random.nextInt(300));
                        pstmt.setFloat(4, random.nextFloat());
                        pstmt.addBatch();
                    }
                    int [] exeResult = pstmt.executeBatch();
                    // you can check exeResult here
                    System.out.println("insert " + exeResult.length + " rows.");
                }
            }
        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed exceptions info
            System.out.println("Failed to insert to table meters using stmt, url: " + jdbcUrl + "; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw ex;
        } catch (Exception ex){
            System.out.println("Failed to insert to table meters using stmt, url: " + jdbcUrl + "; ErrMessage: " + ex.getMessage());
            throw ex;
        }
    }

    private static void init(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS power");
            stmt.execute("USE power");
            stmt.execute("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
        }
    }
}
```

The methods to set TAGS values:

```java
public void setTagNull(int index, int type)
public void setTagBoolean(int index, boolean value)
public void setTagInt(int index, int value)
public void setTagByte(int index, byte value)
public void setTagShort(int index, short value)
public void setTagLong(int index, long value)
public void setTagTimestamp(int index, long value)
public void setTagFloat(int index, float value)
public void setTagDouble(int index, double value)
public void setTagString(int index, String value)
public void setTagNString(int index, String value)
```

The methods to set VALUES columns:

```java
public void setInt(int columnIndex, ArrayList<Integer> list) throws SQLException
public void setFloat(int columnIndex, ArrayList<Float> list) throws SQLException
public void setTimestamp(int columnIndex, ArrayList<Long> list) throws SQLException
public void setLong(int columnIndex, ArrayList<Long> list) throws SQLException
public void setDouble(int columnIndex, ArrayList<Double> list) throws SQLException
public void setBoolean(int columnIndex, ArrayList<Boolean> list) throws SQLException
public void setByte(int columnIndex, ArrayList<Byte> list) throws SQLException
public void setShort(int columnIndex, ArrayList<Short> list) throws SQLException
public void setString(int columnIndex, ArrayList<String> list, int size) throws SQLException
public void setNString(int columnIndex, ArrayList<String> list, int size) throws SQLException
```

### Schemaless Writing

TDengine has added the ability to schemaless writing. It is compatible with InfluxDB's Line Protocol, OpenTSDB's telnet line protocol, and OpenTSDB's JSON format protocol. See [schemaless writing](https://docs.taosdata.com/reference/schemaless/) for details.

**Note**.

- Because the rules for schema-less writing and automatic table creation differ from those in the previous SQL examples, please ensure that the tables `meters`, `metric_telnet`, and `metric_json` do not exist before running the code examples.
- The OpenTSDB TELNET line protocol and OpenTSDB JSON format protocol only support one data column, so we have used other examples.
- The following example code is based on taos-jdbcdriver-3.0.0.

```java
public class SchemalessInsertTest {
    private static final String host = "127.0.0.1";
    private static final String lineDemo = "st,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000";
    private static final String telnetDemo = "stb0_0 1626006833 4 host=host0 interface=eth0";
    private static final String jsonDemo = "{\"metric\": \"meter_current\",\"timestamp\": 1346846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"Beijing\", \"id\": \"d1001\"}}";

    public static void main(String[] args) throws SQLException {
        final String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        try (Connection connection = DriverManager.getConnection(url)) {
            init(connection);

            SchemalessWriter writer = new SchemalessWriter(connection);
            writer.write(lineDemo, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS);
            writer.write(telnetDemo, SchemalessProtocolType.TELNET, SchemalessTimestampType.MILLI_SECONDS);
            writer.write(jsonDemo, SchemalessProtocolType.JSON, SchemalessTimestampType.NOT_CONFIGURED);
        }
    }

    private static void init(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("drop database if exists test_schemaless");
            stmt.executeUpdate("create database if not exists test_schemaless");
            stmt.executeUpdate("use test_schemaless");
        }
    }
}
```

### Subscriptions

TDengine offers data subscription and consumption interfaces similar to those of message queue products. In many scenarios, by adopting the TDengine time-series big data platform, there is no need to integrate additional message queue products, thereby simplifying application design and reducing operational costs.   
The TDengine Java connector supports subscription features. For basic information on data subscription, please refer to the official documentation at [Data Subscription](https://docs.tdengine.com/develop/tmq/).

#### Create subscriptions

Execute the SQL to create a topic through `taos shell` or `taos explore`: `CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM meters;` 
The above SQL will create a subscription named `topic_meters`. Each record in the messages obtained by this subscription is composed of the columns selected by this query statement `SELECT ts, current, voltage, phase, groupid, location FROM meters`.

**Note**: In the TDengine Java connector implementation, there are the following limitations for subscription queries.

- Query statement limitation: Subscription queries can only use the `select` statement and do not support other types of SQL, such as `insert`, `update`, or `delete`, etc.
- Raw data query: Subscription queries can only query raw data and cannot query aggregated or calculated results.
- Time order limitation: Subscription queries can only query data in chronological order.

#### Create Consumer


```java
Properties config = new Properties();
config.setProperty("td.connect.type", "ws");
config.setProperty("bootstrap.servers", "localhost:6041");
config.setProperty("auto.offset.reset", "latest");
config.setProperty("msg.with.table.name", "true");
config.setProperty("enable.auto.commit", "true");
config.setProperty("auto.commit.interval.ms", "1000");
config.setProperty("group.id", "group1");
config.setProperty("client.id", "1");
config.setProperty("td.connect.user", "root");
config.setProperty("td.connect.pass", "taosdata");
config.setProperty("value.deserializer", "com.taosdata.example.WsConsumerLoopFull$ResultDeserializer");
config.setProperty("value.deserializer.encoding", "UTF-8");

try {
    return new TaosConsumer<>(config);
} catch (SQLException ex) {
    // handle any errors, please refer to the JDBC specifications for detailed exceptions info
    System.out.println("Failed to create websocket consumer, host : " + config.getProperty("bootstrap.servers") + "; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
    throw new SQLException("Failed to create consumer", ex);
} catch (Exception ex) {
    System.out.println("Failed to create websocket consumer, host : " + config.getProperty("bootstrap.servers")
            + "; ErrMessage: " + ex.getMessage());
    throw new SQLException("Failed to create consumer", ex);
}
```

- `enable.auto.commit`: Whether to allow automatic commit.
- `group.id`: The group that the consumer belongs to.
- `client.id`: Client ID, client IDs with the same group ID will share consumption.
- value.deserializer: The result set deserialization method, you can implement `com.taosdata.jdbc.tmq.ReferenceDeserializer`, and specify the result set bean to achieve deserialization. You can also inherit `com.taosdata.jdbc.tmq.Deserializer` to customize the deserialization method according to the resultSet of SQL.
  For other parameters, please refer to: [Consumer parameters](https://docs.tdengine.com/develop/tmq/#create-a-consumer)

#### poll data


```java
try {
    List<String> topics = Collections.singletonList("topic_meters");

    // subscribe to the topics
    consumer.subscribe(topics);
    System.out.println("subscribe topics successfully");
    for (int i = 0; i < 50; i++) {
        // poll data
        ConsumerRecords<ResultBean> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<ResultBean> record : records) {
            ResultBean bean = record.value();
            // process the data here
            System.out.println("data: " + JSON.toJSONString(bean));
        }
    }

} catch (SQLException ex) {
    // handle any errors, please refer to the JDBC specifications for detailed exceptions info
    System.out.println("Failed to poll data; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
    throw new SQLException("Failed to poll data", ex);
} catch (Exception ex) {
    System.out.println("Failed to poll data; ErrMessage: " + ex.getMessage());
    throw new SQLException("Failed to poll data", ex);
}
```

- The parameter of the `subscribe` method: the list of topics name to subscribe to, supporting subscription to multiple topics simultaneously.
- `poll` retrieves a message on each call, and a message may contain multiple records.
- `ResultBean` is a custom internal class we defined, whose field names and data types correspond one-to-one with the names and data types of the columns. This way, objects of type `ResultBean` can be deserialized according to the deserialization class corresponding to the `value.deserializer` property.


#### Close subscriptions

```java
try {
    // unsubscribe the consumer
    consumer.unsubscribe();
} catch (SQLException ex) {
    // handle any errors, please refer to the JDBC specifications for detailed exceptions info
    System.out.println("Failed to unsubscribe consumer. ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
    throw new SQLException("Failed to unsubscribe consumer", ex);
} catch (Exception ex) {
    System.out.println("Failed to unsubscribe consumer. ErrMessage: " + ex.getMessage());
    throw new SQLException("Failed to unsubscribe consumer", ex);
}
finally {
    // close the consumer
    consumer.close();
}
```

#### example:

```java
public class WsConsumerLoopFull {
    static private Connection connection;
    static private Statement statement;
    static private volatile boolean stopThread = false;

    public static TaosConsumer<ResultBean> getConsumer() throws SQLException {
        Properties config = new Properties();
        config.setProperty("td.connect.type", "ws");
        config.setProperty("bootstrap.servers", "localhost:6041");
        config.setProperty("auto.offset.reset", "latest");
        config.setProperty("msg.with.table.name", "true");
        config.setProperty("enable.auto.commit", "true");
        config.setProperty("auto.commit.interval.ms", "1000");
        config.setProperty("group.id", "group1");
        config.setProperty("client.id", "1");
        config.setProperty("td.connect.user", "root");
        config.setProperty("td.connect.pass", "taosdata");
        config.setProperty("value.deserializer", "com.taosdata.example.WsConsumerLoopFull$ResultDeserializer");
        config.setProperty("value.deserializer.encoding", "UTF-8");

        try {
            return new TaosConsumer<>(config);
        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed exceptions info
            System.out.println("Failed to create websocket consumer, host : " + config.getProperty("bootstrap.servers") + "; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to create consumer", ex);
        } catch (Exception ex) {
            System.out.println("Failed to create websocket consumer, host : " + config.getProperty("bootstrap.servers")
                    + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to create consumer", ex);
        }
    }

    public static void pollExample(TaosConsumer<ResultBean> consumer) throws SQLException {
        try {
            List<String> topics = Collections.singletonList("topic_meters");

            // subscribe to the topics
            consumer.subscribe(topics);
            System.out.println("subscribe topics successfully");
            for (int i = 0; i < 50; i++) {
                // poll data
                ConsumerRecords<ResultBean> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<ResultBean> record : records) {
                    ResultBean bean = record.value();
                    // process the data here
                    System.out.println("data: " + JSON.toJSONString(bean));
                }
            }

        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed exceptions info
            System.out.println("Failed to poll data; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to poll data", ex);
        } catch (Exception ex) {
            System.out.println("Failed to poll data; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to poll data", ex);
        }
    }

    public static void seekExample(TaosConsumer<ResultBean> consumer) throws SQLException {
        try {
            List<String> topics = Collections.singletonList("topic_meters");

            // subscribe to the topics
            consumer.subscribe(topics);
            System.out.println("subscribe topics successfully");
            Set<TopicPartition> assignment = consumer.assignment();
            System.out.println("now assignment: " + JSON.toJSONString(assignment));

            ConsumerRecords<ResultBean> records = ConsumerRecords.emptyRecord();
            // make sure we have got some data
            while (records.isEmpty()) {
                records = consumer.poll(Duration.ofMillis(100));
            }

            consumer.seekToBeginning(assignment);
            System.out.println("assignment seek to beginning successfully");
            System.out.println("beginning assignment: " + JSON.toJSONString(assignment));
        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed exceptions info
            System.out.println("seek example failed; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("seek example failed", ex);
        } catch (Exception ex) {
            System.out.println("seek example failed; ErrMessage: " + ex.getMessage());
            throw new SQLException("seek example failed", ex);
        }
    }


    public static void commitExample(TaosConsumer<ResultBean> consumer) throws SQLException {
        try {
            List<String> topics = Collections.singletonList("topic_meters");

            consumer.subscribe(topics);
            for (int i = 0; i < 50; i++) {
                ConsumerRecords<ResultBean> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<ResultBean> record : records) {
                    ResultBean bean = record.value();
                    // process your data here
                    System.out.println("data: " + JSON.toJSONString(bean));
                }
                if (!records.isEmpty()) {
                    // after processing the data, commit the offset manually
                    consumer.commitSync();
                }
            }
        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed exceptions info
            System.out.println("Failed to execute consumer functions. ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to execute consumer functions", ex);
        } catch (Exception ex) {
            System.out.println("Failed to execute consumer functions. ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to execute consumer functions", ex);
        }
    }

    public static void unsubscribeExample(TaosConsumer<ResultBean> consumer) throws SQLException {
        List<String> topics = Collections.singletonList("topic_meters");
        consumer.subscribe(topics);
        try {
            // unsubscribe the consumer
            consumer.unsubscribe();
        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed exceptions info
            System.out.println("Failed to unsubscribe consumer. ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to unsubscribe consumer", ex);
        } catch (Exception ex) {
            System.out.println("Failed to unsubscribe consumer. ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to unsubscribe consumer", ex);
        }
        finally {
            // close the consumer
            consumer.close();
        }
    }

    public static class ResultDeserializer extends ReferenceDeserializer<ResultBean> {

    }

    // use this class to define the data structure of the result record
    public static class ResultBean {
        private Timestamp ts;
        private double current;
        private int voltage;
        private double phase;
        private int groupid;
        private String location;

        public Timestamp getTs() {
            return ts;
        }

        public void setTs(Timestamp ts) {
            this.ts = ts;
        }

        public double getCurrent() {
            return current;
        }

        public void setCurrent(double current) {
            this.current = current;
        }

        public int getVoltage() {
            return voltage;
        }

        public void setVoltage(int voltage) {
            this.voltage = voltage;
        }

        public double getPhase() {
            return phase;
        }

        public void setPhase(double phase) {
            this.phase = phase;
        }

        public int getGroupid() {
            return groupid;
        }

        public void setGroupid(int groupid) {
            this.groupid = groupid;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }
    }

    public static void prepareData() throws SQLException, InterruptedException {
        try {
            int i = 0;
            while (!stopThread) {
                String insertQuery = "INSERT INTO power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') VALUES (NOW + " + i + "a, 10.30000, 219, 0.31000) ";
                int affectedRows = statement.executeUpdate(insertQuery);
                assert affectedRows == 1;
                i++;
                Thread.sleep(1);
            }
        } catch (SQLException ex) {
            System.out.println("Failed to insert data to power.meters, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to insert data to power.meters", ex);
        }
    }

    public static void prepareMeta() throws SQLException {
        try {
            statement.executeUpdate("CREATE DATABASE IF NOT EXISTS power");
            statement.executeUpdate("USE power");
            statement.executeUpdate("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
            statement.executeUpdate("CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM meters");
        } catch (SQLException ex) {
            System.out.println("Failed to create db and table, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to create db and table", ex);
        }
    }

    public static void initConnection() throws SQLException {
        String url = "jdbc:TAOS://localhost:6030?user=root&password=taosdata";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");

        try {
            connection = DriverManager.getConnection(url, properties);
        } catch (SQLException ex) {
            System.out.println("Failed to create connection, url:" + url + "; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to create connection", ex);
        }
        try {
            statement = connection.createStatement();
        } catch (SQLException ex) {
            System.out.println("Failed to create statement, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to create statement", ex);
        }
        System.out.println("Connection created successfully.");
    }

    public static void closeConnection() throws SQLException {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException ex) {
            System.out.println("Failed to close statement, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to close statement", ex);
        }

        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ex) {
            System.out.println("Failed to close connection, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to close connection", ex);
        }
        System.out.println("Connection closed Successfully.");
    }


    public static void main(String[] args) throws SQLException, InterruptedException {
        initConnection();
        prepareMeta();

        // create a single thread executor
        ExecutorService executor = Executors.newSingleThreadExecutor();

        // submit a task
        executor.submit(() -> {
            try {
                prepareData();
            } catch (SQLException ex) {
                System.out.println("Failed to prepare data, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
                return;
            } catch (Exception ex) {
                System.out.println("Failed to prepare data, ErrMessage: " + ex.getMessage());
                return;
            }
            System.out.println("pollDataExample executed successfully");
        });

        try {
            TaosConsumer<ResultBean> consumer = getConsumer();

            pollExample(consumer);
            System.out.println("pollExample executed successfully");
            consumer.unsubscribe();

            seekExample(consumer);
            System.out.println("seekExample executed successfully");
            consumer.unsubscribe();

            commitExample(consumer);
            System.out.println("commitExample executed successfully");
            consumer.unsubscribe();

            unsubscribeExample(consumer);
            System.out.println("unsubscribeExample executed successfully");

        } catch (SQLException ex) {
            System.out.println("Failed to poll data from topic_meters, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            return;
        } catch (Exception ex) {
            System.out.println("Failed to poll data from topic_meters, ErrMessage: " + ex.getMessage());
            return;
        }

        stopThread = true;
        // close the executor, which will make the executor reject new tasks
        executor.shutdown();

        try {
            // wait for the executor to terminate
            boolean result = executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            assert result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Wait executor termination failed.");
        }

        closeConnection();
        System.out.println("program end.");
    }
}
```

### Use with connection pool

#### HikariCP

Example usage is as follows.

```java
 public static void main(String[] args) throws SQLException {
    HikariConfig config = new HikariConfig();
    // jdbc properties
    config.setJdbcUrl("jdbc:TAOS://127.0.0.1:6030/log");
    config.setUsername("root");
    config.setPassword("taosdata");
    // connection pool configurations
    config.setMinimumIdle(10);           //minimum number of idle connection
    config.setMaximumPoolSize(10);      //maximum number of connection in the pool
    config.setConnectionTimeout(30000); //maximum wait milliseconds for get connection from pool
    config.setMaxLifetime(0);       // maximum life time for each connection
    config.setIdleTimeout(0);       // max idle time for recycle idle connection
    config.setConnectionTestQuery("select server_status()"); //validation query

    HikariDataSource ds = new HikariDataSource(config); //create datasource

    Connection  connection = ds.getConnection(); // get connection
    Statement statement = connection.createStatement(); // get statement

    //query or insert
    // ...

    connection.close(); // put back to connection pool
}
```

> getConnection(), you need to call the close() method after you finish using it. It doesn't close the connection. It just puts it back into the connection pool.
> For more questions about using HikariCP, please see the [official instructions](https://github.com/brettwooldridge/HikariCP).

#### Druid

Example usage is as follows.

```java
public static void main(String[] args) throws Exception {

    DruidDataSource dataSource = new DruidDataSource();
    // jdbc properties
    dataSource.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
    dataSource.setUrl(url);
    dataSource.setUsername("root");
    dataSource.setPassword("taosdata");
    // pool configurations
    dataSource.setInitialSize(10);
    dataSource.setMinIdle(10);
    dataSource.setMaxActive(10);
    dataSource.setMaxWait(30000);
    dataSource.setValidationQuery("select server_status()");

    Connection  connection = dataSource.getConnection(); // get connection
    Statement statement = connection.createStatement(); // get statement
    //query or insert
    // ...

    connection.close(); // put back to connection pool
}
```

> For more questions about using druid, please see [Official Instructions](https://github.com/alibaba/druid).

### More sample programs

The source code of the sample application is under `TDengine/examples/JDBC`:

- JDBCDemo: JDBC sample source code.
- JDBCConnectorChecker: JDBC installation checker source and jar package.
- connectionPools: using taos-jdbcdriver in connection pools such as HikariCP, Druid, dbcp, c3p0, etc.
- SpringJdbcTemplate: using taos-jdbcdriver in Spring JdbcTemplate.
- mybatisplus-demo: using taos-jdbcdriver in Springboot + Mybatis.

Please refer to: [JDBC example](https://github.com/taosdata/TDengine/tree/3.0/examples/JDBC)

## Recent update logs


| taos-jdbcdriver version |                                                                                                                                                                                                                major changes                                                                                                                                                                                                                 | TDengine version |
| :---------------------: | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------: | :--------------: |
|          3.3.0          |                                                                                                                                                    1. Optimized data transmission performance under Websocket connection; 2. SSL validation skipping is supported but disabled by default                                                                                                                                                    | 3.3.2.0 or later |
|         3.2.11          |                                                                                                                                                                                       Fixed the result set closing bug when using a native connection.                                                                                                                                                                                       |        -         |
|         3.2.10          | 1. Automatic compression/decompression for data transmission, disabled by default; 2.Automatic reconnection for websocket with configurable parameter, disabled by default; 3. A new method for schemaless writing is added in the connection class; 4. Optimized performance for data fetching on native connection; 5. Fixing for some known issues; 6. The list of supported functions can be returned by the API for retrieving metadata |        -         |
|          3.2.9          |                                                                                                                                                                                                Fixed websocket prepareStatement closing bug.                                                                                                                                                                                                 |        -         |
|          3.2.8          |                                                                                                                                              Improved autocommit, fixed commit offset on websocket connection bug, websocket prepareStatement uses one connection and meta data supports view.                                                                                                                                               |        -         |
|          3.2.7          |                                                                                                                                                         Support VARBINARY and GEOMETRY types, and add time zone support for native connections. Support websocket auto reconnection                                                                                                                                                          | 3.2.0.0 or later |
|          3.2.5          |                                                                                                                                                                                             Subscription add committed() and assignment() method                                                                                                                                                                                             | 3.1.0.3 or later |
|          3.2.4          |                                                                                                                                                                  Subscription add the enable.auto.commit parameter and the unsubscribe() method in the WebSocket connection                                                                                                                                                                  |        -         |
|          3.2.3          |                                                                                                                                                                                              Fixed resultSet data parsing failure in some cases                                                                                                                                                                                              |        -         |
|          3.2.2          |                                                                                                                                                                                                        Subscription add seek function                                                                                                                                                                                                        | 3.0.5.0 or later |
|          3.2.1          |                                                                                                                                                                                   JDBC REST connection supports schemaless/prepareStatement over WebSocket                                                                                                                                                                                   | 3.0.3.0 or later |
|          3.2.0          |                                                                                                                                                                                                       This version has been deprecated                                                                                                                                                                                                       |        -         |
|          3.1.0          |                                                                                                                                                                                          JDBC REST connection supports subscription over WebSocket                                                                                                                                                                                           |        -         |
|      3.0.1 - 3.0.4      |                                                                                                                                         Fixed the issue of result set data sometimes parsing incorrectly. 3.0.1 is compiled on JDK 11, you are advised to use other version in the JDK 8 environment                                                                                                                                         |        -         |
|          3.0.0          |                                                                                                                                                                                                           Support for TDengine 3.0                                                                                                                                                                                                           | 3.0.0.0 or later |
|         2.0.42          |                                                                                                                                                                                          Fix wasNull interface return value in WebSocket connection                                                                                                                                                                                          |        -         |
|         2.0.41          |                                                                                                                                                                                        Fix decode method of username and password in REST connection                                                                                                                                                                                         |        -         |
|     2.0.39 - 2.0.40     |                                                                                                                                                                                                Add REST connection/request timeout parameters                                                                                                                                                                                                |        -         |
|         2.0.38          |                                                                                                                                                                                                 JDBC REST connections add bulk pull function                                                                                                                                                                                                 |        -         |
|         2.0.37          |                                                                                                                                                                                                              Support json tags                                                                                                                                                                                                               |        -         |
|         2.0.36          |                                                                                                                                                                                                          Support schemaless writing                                                                                                                                                                                                          |        -         |

## Frequently Asked Questions

1. Why is there no performance improvement when using Statement's `addBatch()` and `executeBatch()` to perform `batch data writing/update`?

   **Cause**: In TDengine's JDBC implementation, SQL statements submitted by `addBatch()` method are executed sequentially in the order they are added, which does not reduce the number of interactions with the server and does not bring performance improvement.

   **Solution**: 1. splice multiple values in a single insert statement; 2. use multi-threaded concurrent insertion; 3. use parameter-bound writing

2. java.lang.UnsatisfiedLinkError: no taos in java.library.path

   **Cause**: The program did not find the dependent native library `taos`.

   **Solution**: On Windows you can copy `C:\TDengine\driver\taos.dll` to the `C:\Windows\System32` directory, on Linux the following soft link will be created `ln -s /usr/local/taos/driver/libtaos.so.x.x.x.x /usr/lib/libtaos.so` will work.

3. java.lang.UnsatisfiedLinkError: taos.dll Can't load AMD 64 bit on an IA 32-bit platform

   **Cause**: Currently, TDengine only supports 64-bit JDK.

   **Solution**: Reinstall the 64-bit JDK.

4. java.lang.NoSuchMethodError: setByteArray

   **Cause**: `taos-jdbcdriver` version 3.* only supports TDengine 3.0 or above.

   **Solution**: connect TDengine 2.* using `taos-jdbcdriver` 2.* version.

For other questions, please refer to [FAQ](https://docs.taosdata.com/train-faq/faq/)

