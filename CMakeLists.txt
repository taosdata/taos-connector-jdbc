# Copyright (c) 2019 TAOS Data, Inc.
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

CMAKE_MINIMUM_REQUIRED(VERSION 3.0...3.20)

PROJECT(TDengine)

IF (TD_MVN_INSTALLED)
  SET(JDBC_CMD_NAME "jdbc_cmd")
  SET(JDBC_TARGET_NAME "jdbc_target")  
  SET(_output "${CMAKE_CURRENT_BINARY_DIR}/${JDBC_CMD_NAME}")
  file(GLOB_RECURSE _depends "${CMAKE_CURRENT_SOURCE_DIR}/src/*")
  ADD_CUSTOM_COMMAND(OUTPUT ${_output}
    POST_BUILD
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/pom.xml
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/deploy-pom.xml
    DEPENDS ${_depends}
    COMMAND mvn -Dmaven.test.skip=true install -f ${CMAKE_CURRENT_SOURCE_DIR}/pom.xml
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/target/taos-jdbcdriver-2.0.38-dist.jar ${LIBRARY_OUTPUT_PATH}
    COMMAND mvn -Dmaven.test.skip=true clean -f ${CMAKE_CURRENT_SOURCE_DIR}/pom.xml
    COMMAND ${CMAKE_COMMAND} -E touch "${_output}"
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    VERBATIM
    COMMENT "build jdbc driver")
  ADD_CUSTOM_TARGET(${JDBC_TARGET_NAME} ALL WORKING_DIRECTORY ${EXECUTABLE_OUTPUT_PATH} DEPENDS ${JDBC_CMD_NAME})
  UNSET(_depends)
  UNSET(_output)
ENDIF ()
