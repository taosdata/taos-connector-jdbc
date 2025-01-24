# TDengine Java Connector


| GitHub Action Tests                                                                                  | CodeCov                                                                                                                                               |
| ---------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| ![actions](https://github.com/taosdata/taos-connector-jdbc/actions/workflows/version3.yml/badge.svg) | [![codecov](https://codecov.io/gh/taosdata/taos-connector-jdbc/graph/badge.svg?token=GQRD9WCQ64)](https://codecov.io/gh/taosdata/taos-connector-jdbc) |


English | [简体中文](./README-CN.md)

- [TDengine Java Connector](#tdengine-java-connector)
  - [Introduction](#introduction)
    - [Connection Methods](#connection-methods)
    - [JDBC and JRE Version Compatibility](#jdbc-and-jre-version-compatibility)
    - [Supported Platforms](#supported-platforms)
  - [Getting the Driver](#getting-the-driver)
    - [Pre-installation](#pre-installation)
    - [Installing the Driver](#installing-the-driver)
      - [Maven Project](#maven-project)
      - [Gradle Project](#gradle-project)
  - [Documentation](#documentation)
  - [Prerequisites](#prerequisites)
  - [Build](#build)
  - [Testing](#testing)
    - [Run tests](#run-tests)
    - [Adding Test Cases](#adding-test-cases)
  - [Submitting Issues](#submitting-issues)
  - [Submitting PRs](#submitting-prs)
  - [References](#references)
  - [License](#license)


## Introduction
`taos-jdbcdriver` is the official Java connector for TDengine, allowing Java developers to develop applications that access the TDengine database. `taos-jdbcdriver` implements the standard interfaces of the JDBC driver and supports functions such as data writing, querying, subscription, schemaless writing, and parameter binding.

### Connection Methods
`taos-jdbcdriver` provides three connection methods:
- Native Connection: Establishes a connection directly with the server program taosd through the client driver taosc. This method requires that the versions of the client driver taosc and the server taosd are consistent.
- REST Connection: Establishes a connection with taosd through the REST API provided by the taosAdapter component. This method doesn't perform as well as the native connection in supporting large-scale data queries.
- WebSocket Connection: Establishes a connection with taosd through the WebSocket API provided by the taosAdapter component. It does not rely on the TDengine client driver, can be cross-platform, is more convenient and flexible, and its performance is close to that of the native connection.

We recommend using the WebSocket connection method. For detailed instructions, please refer to: [Connection Methods](https://docs.tdengine.com/developer-guide/connecting-to-tdengine/#connection-methods).

### JDBC and JRE Version Compatibility
- JDBC: Supports JDBC 4.2 and above.
- JRE: Supports JRE 8 and above.

### Supported Platforms
- The platforms supported by the native connection are consistent with those supported by the TDengine client driver.
- WebSocket/REST connections support all platforms that can run Java.

## Getting the Driver

### Pre-installation 

Before using the Java Connector to connect to the database, the following conditions must be met:

- Java 1.8 or above runtime environment and Maven 3.6 or above installed.
- Ensure that the TDengine client driver is installed before using the native connection (no installation is required for WebSocket/REST connections). For specific steps, please refer to [Install Client Driver](https://docs.tdengine.com/developer-guide/connecting-to-tdengine/#installing-the-client-driver-taosc).

### Installing the Driver

The taos-jdbcdriver has been released to the [Sonatype Repository](https://search.maven.org/artifact/com.taosdata.jdbc/taos-jdbcdriver) and synchronized to major repositories.

- [sonatype](https://search.maven.org/artifact/com.taosdata.jdbc/taos-jdbcdriver)
- [mvnrepository](https://mvnrepository.com/artifact/com.taosdata.jdbc/taos-jdbcdriver)
- [maven.aliyun](https://maven.aliyun.com/mvn/search)

#### Maven Project
Add the following dependency to your pom.xml:

```xml-dtd
<dependency>
 <groupId>com.taosdata.jdbc</groupId>
 <artifactId>taos-jdbcdriver</artifactId>
 <version>3.5.3</version>
</dependency>
```

#### Gradle Project

If your build.gradle file uses Groovy syntax:
  ```xml-dtd
  dependencies {
      implementation 'com.taosdata.jdbc:taos-jdbcdriver:3.5.3'
  }
  ```
If your build.gradle.kts file uses Kotlin syntax:
  ```xml-dtd
  dependencies {
      implementation("com.taosdata.jdbc:taos-jdbcdriver:3.5.3")
  }
  ```

## Documentation  
- For development examples, see [Developer Guide](https://docs.tdengine.com/developer-guide/), which includes examples of data writing, querying, schemaless writing, parameter binding, and data subscription.
- For other reference information, see [Reference Manual](https://docs.tdengine.com/tdengine-reference/client-libraries/java/), which includes version history, data types, example programs, API descriptions, and FAQs.

## Prerequisites
- Java 1.8 or above runtime environment and Maven 3.6 or above installed, with environment variables correctly set.
- TDengine has been deployed locally. For specific steps, please refer to [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/), and taosd and taosAdapter have been started.

## Build
Execute `mvn clean package` in the project directory to build the project.

## Testing
### Run tests
Execute `mvn test` in the project directory to run the tests. The test cases will connect to the local TDengine server and taosAdapter for testing.
After running the tests, the result similar to the following will be printed eventually. If all test cases pass, both Failures and Errors will be 0.
```
[INFO] Results:
[INFO] 
[WARNING] Tests run: 2353, Failures: 0, Errors: 0, Skipped: 16
```

### Adding Test Cases
All tests are located in the `src/test/java/com/taosdata/jdbc` directory of the project. The directory is divided according to the functions being tested. You can add new test files or add test cases in existing test files.
The test cases use the JUnit framework. Generally, a connection is established and a database is created in the `before` method, and the database is droped and the connection is released in the `after` method.

## Submitting Issues
We welcome the submission of [GitHub Issue](https://github.com/taosdata/taos-connector-jdbc/issues/new?template=Blank+issue). When submitting, please provide the following information:

- Problem description, whether it always occurs, and it's best to include a detailed call stack.
- JDBC driver version.
- JDBC connection parameters (username and password not required).
- TDengine server version.

## Submitting PRs
We welcome developers to contribute to this project. When submitting PRs, please follow these steps:

1. Fork this project, refer to ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo)).
1. Create a new branch from the main branch with a meaningful branch name (`git checkout -b my_branch`). Do not modify the main branch directly.
1. Modify the code, ensure all unit tests pass, and add new unit tests to verify the changes.
1. Push the changes to the remote branch (`git push origin my_branch`).
1. Create a Pull Request on GitHub ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)).
1. After submitting the PR, if CI passes, you can find your PR on the [codecov](https://app.codecov.io/gh/taosdata/taos-connector-jdbc/pulls) page to check the test coverage.

## References
[TDengine Official Website](https://www.tdengine.com/)
[TDengine GitHub](https://github.com/taosdata/TDengine)

## License
[MIT License](./LICENSE)
