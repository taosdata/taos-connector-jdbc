# TDengine Java Connector

| Github Action Tests                                                                                  | CodeCov                                                                                                                                               |
| ---------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| ![actions](https://github.com/taosdata/taos-connector-jdbc/actions/workflows/version3.yml/badge.svg) | [![codecov](https://codecov.io/gh/taosdata/taos-connector-jdbc/graph/badge.svg?token=GQRD9WCQ64)](https://codecov.io/gh/taosdata/taos-connector-jdbc) |


简体中文 | [English](./README.md)

## 简介

`taos-jdbcdriver` 是 TDengine 的官方 Java 语言连接器，Java 开发人员可以通过它开发存取 TDengine 数据库的应用软件。`taos-jdbcdriver` 实现了 JDBC driver 标准的接口，支持数据写入、查询、订阅、schemaless 接口和参数绑定接口等功能。  

### 连接方式
`taos-jdbcdriver` 提供了三种连接方式：
- 原生连接：通过客户端驱动程序 taosc 直接与服务端程序 taosd 建立连接。这种方式需要保证客户端的驱动程序 taosc 和服务端的 taosd 版本保持一致。
- REST 连接：通过 taosAdapter 组件提供的 REST API 建立与 taosd 的连接。这种方式在大批量数据查询支持方面性能不如原生连接。
- Websocket 连接： 通过 taosAdapter 组件提供的 WebSocket API 建立与 taosd 的连接。不依赖 TDengine 客户端驱动，可以跨平台，更加方便灵活，性能与原生连接接近。  

我们推荐使用 WebSocket 连接方式。 详细说明请参考：[连接方式](https://docs.taosdata.com/develop/connect/#%E8%BF%9E%E6%8E%A5%E6%96%B9%E5%BC%8F) 。


### JDBC 和 JRE 版本兼容性
- JDBC: 支持 JDBC 4.2 及以上版本。
- JRE: 支持 JRE 8 及以上版本。

### 支持的平台
- 原生连接支持的平台和 TDengine 客户端驱动支持的平台一致。
- WebSocket/REST 连接支持所有能运行 Java 的平台。


## 获取驱动

### 安装前准备

使用 Java Connector 连接数据库前，需要具备以下条件：

- 已安装 Java 1.8 或以上版本运行时环境和 Maven 3.6 或以上版本
- 使用原生连接前保证已安装 TDengine 客户端驱动（若使用 Websocket/REST 连接无需安装），具体步骤请参考 [安装客户端驱动](https://docs.taosdata.com/develop/connect/#%E5%AE%89%E8%A3%85%E5%AE%A2%E6%88%B7%E7%AB%AF%E9%A9%B1%E5%8A%A8-taosc)

### 安装连接器

目前 taos-jdbcdriver 已经发布到 [Sonatype Repository](https://search.maven.org/artifact/com.taosdata.jdbc/taos-jdbcdriver) 仓库，且各大仓库都已同步。

- [sonatype](https://search.maven.org/artifact/com.taosdata.jdbc/taos-jdbcdriver)
- [mvnrepository](https://mvnrepository.com/artifact/com.taosdata.jdbc/taos-jdbcdriver)
- [maven.aliyun](https://maven.aliyun.com/mvn/search)

#### Maven 项目
在 pom.xml 中添加以下依赖：

```xml-dtd
<dependency>
 <groupId>com.taosdata.jdbc</groupId>
 <artifactId>taos-jdbcdriver</artifactId>
 <version>3.5.3</version>
</dependency>
```

#### Gradle 项目
如果你的 build.gradle 文件采用 Groovy 语法：
  ```xml-dtd
  dependencies {
      implementation 'com.taosdata.jdbc:taos-jdbcdriver:3.5.3'
  }
  ```

如果你的 build.gradle.kts 文件使用 Kotlin 语法：
  ```xml-dtd
  dependencies {
      implementation("com.taosdata.jdbc:taos-jdbcdriver:3.5.3")
  }
  ```

## 文档
- 开发示例请见 [开发指南](https://docs.taosdata.com/develop/)，包含了数据写入、查询、无模式写入、参数绑定和数据订阅等示例。
- 其他参考信息请见 [参考手册](https://docs.taosdata.com/reference/connector/java/)，包含了版本历史、数据类型、示例程序汇总、API 说明和常见问题等。


## 前置条件

- 已安装 Java 1.8 或以上版本运行时环境和 Maven 3.6 或以上版本，且正确设置了环境变量。
- 本地已经部署 TDengine，具体步骤请参考 [部署服务端](https://docs.taosdata.com/get-started/package/)，且已经启动 taosd 与 taosAdapter。

## 构建

项目目录下执行 `mvn clean package` 构建项目。

## 测试
项目目录下执行 `mvn test` 运行测试，测试用例会连接到本地的 TDengine 服务器与 taosAdapter 进行测试。

## 提交 Issue
我们欢迎提交 [Github Issue](https://github.com/taosdata/taos-connector-jdbc/issues/new?template=Blank+issue)。 提交时请说明下面信息：
- 问题描述，是否必现，最好能包含详细调用堆栈。
- JDBC 驱动版本。
- JDBC 连接参数（不需要用户名密码）。
- TDengine 服务端版本。

## 提交 PR
我们欢迎开发者一起开发本项目，提交 PR 时请参考下面步骤：
1. Fork 本项目，请参考 ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo))。
1. 从 main 分支创建一个新分支，请使用有意义的分支名称 (`git checkout -b my_branch`)。注意不要直接在 main 分支上修改。
1. 修改代码，保证所有单元测试通过，并增加新的单元测试验证修改。
1. 提交修改到远端分支 (`git push origin my_branch`)。
1. 在 GitHub 上创建一个 Pull Request ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request))。
1. 提交 PR 后，如果 CI 通过，可以在 [codecov](https://app.codecov.io/gh/taosdata/taos-connector-jdbc/pulls) 页面找到自己 PR，看单测覆盖率。

## 引用

- [TDengine 官网](https://www.taosdata.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 许可证

[MIT License](./LICENSE)
