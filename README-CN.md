# TDengine Java Connector

| Github Action Tests                                                                                  | CodeCov                                                                                                                                               |
| ---------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| ![actions](https://github.com/taosdata/taos-connector-jdbc/actions/workflows/version3.yml/badge.svg) | [![codecov](https://codecov.io/gh/taosdata/taos-connector-jdbc/graph/badge.svg?token=GQRD9WCQ64)](https://codecov.io/gh/taosdata/taos-connector-jdbc) |


简体中文 | [English](./README.md)

## 简介

`taos-jdbcdriver` 是 TDengine 的官方 Java 语言连接器，Java 开发人员可以通过它开发存取 TDengine 数据库的应用软件。`taos-jdbcdriver` 实现了 JDBC driver 标准的接口，支持数据写入、查询、订阅、schemaless 接口和参数绑定接口等功能。  
`taos-jdbcdriver` 提供了三种连接方式：
- 原生连接：通过客户端驱动程序 taosc 直接与服务端程序 taosd 建立连接。这种方式需要保证客户端的驱动程序 taosc 和服务端的 taosd 版本保持一致。
- REST 连接：通过 taosAdapter 组件提供的 REST API 建立与 taosd 的连接。这种方式大批量数据查询支持不好。
- Websocket 连接： 通过 taosAdapter 组件提供的 WebSocket API 建立与 taosd 的连接。不依赖 TDengine 客户端驱动，可以跨平台，更加方便灵活，性能与原生连接接近。  

我们推荐使用 WebSocket 连接方式。 详细说明请参考：https://docs.taosdata.com/develop/connect/ 。


### JDBC 和 JRE 版本兼容性
- JDBC: 支持 JDBC 4.2 及以上版本。
- JRE: 支持 JRE 8 及以上版本。

### 支持的平台
- 原生连接支持的平台和 TDengine 客户端驱动支持的平台一致。
- WebSocket/REST 连接支持所有能运行 Java 的平台。


## 获取驱动


### 前置条件

使用 Java Connector 连接数据库前，需要具备以下条件：

- 已安装 Java 1.8 或以上版本运行时环境和 Maven 3.6 或以上版本
- 使用原生连接前保证已安装 TDengine 客户端驱动（若使用 Websocket/REST 连接无需安装），具体步骤请参考[安装客户端驱动](https://docs.taosdata.com/connector/#安装客户端驱动)

### 安装连接器

目前 taos-jdbcdriver 已经发布到 [Sonatype Repository](https://search.maven.org/artifact/com.taosdata.jdbc/taos-jdbcdriver) 仓库，且各大仓库都已同步。

- [sonatype](https://search.maven.org/artifact/com.taosdata.jdbc/taos-jdbcdriver)
- [mvnrepository](https://mvnrepository.com/artifact/com.taosdata.jdbc/taos-jdbcdriver)
- [maven.aliyun](https://maven.aliyun.com/mvn/search)

Maven 项目中，在 pom.xml 中添加以下依赖：

```xml-dtd
<dependency>
 <groupId>com.taosdata.jdbc</groupId>
 <artifactId>taos-jdbcdriver</artifactId>
 <version>3.5.3</version>
</dependency>
```

Gradle 项目中：
- 如果你的 build.gradle 文件采用 Groovy 语法：
  ```xml-dtd
  dependencies {
    implementation 'com.taosdata.jdbc:taos-jdbcdriver:3.5.3'
  }
  ```

- 如果你的 build.gradle.kts 文件使用 Kotlin 语法：
  ```xml-dtd
  dependencies {
      implementation("com.taosdata.jdbc:taos-jdbcdriver:3.5.3")
  }
  ```

## 文档
- 开发示例请见 [开发指南](https://docs.taosdata.com/develop/)，包含了数据写入、查询、无模式写入、参数绑定和数据订阅等示例。
- 其他参考信息请见 [参考手册](https://docs.taosdata.com/reference/connector/java/)，包含了版本历史、数据类型、示例程序汇总、API 说明和常见问题等。


## 贡献

我们鼓励开发者帮助改进这个项目，下面以 Maven 为例，说明本地构建和测试这个项目的流程。

### 前置条件

- 已安装 Java 1.8 或以上版本运行时环境和 Maven 3.6 或以上版本，且正确设置了环境变量。
- 本地已经部署 TDengine，具体步骤请参考[部署服务端](https://docs.taosdata.com/get-started/package/)，且已经启动 taosd 与 taosAdapter。

### 构建

项目目录下执行 `mvn clean package` 构建项目。

### 测试

项目目录下执行 `mvn test` 运行测试，测试用例会连接到本地的 TDengine 服务器与 taosAdapter 进行测试。

## 引用

- [TDengine 官网](https://www.taosdata.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 许可证

[MIT License](./LICENSE)
