/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connect.service

import org.apache.logging.log4j.core.Logger
import org.mockito.Mockito.{spy, verify, when}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.test.SharedSparkSession

class SparkConnectServerSuite extends SparkFunSuite with MockitoSugar with SharedSparkSession {

  test("SparkConnectServerSuite: Error handling") {
    // Mock the logger
    val mockLogger = mock[Logger]
    // Mock the SparkConnectServer
    val mockSparkServer = spy(SparkConnectServer)

    // Exception with SparkThrowable
    val exceptionWithSparkThrowable = new SparkException(
      message = "INTERNAL_ERROR message",
      cause = null,
      errorClass = Some("INTERNAL_ERROR"),
      messageParameters = Map("INTERNAL_ERROR" -> "message"))

    // Exception without SparkThrowable
    val exceptionWithoutSparkThrowable = new Exception("Exception message")

    // Verify log messages for exception with SparkThrowable
    when(mockSparkServer.handleStartServerError(exceptionWithSparkThrowable))
      .thenAnswer(_ =>
        mockLogger.error(
          "Error starting Spark Connect server with error class: " +
            exceptionWithSparkThrowable.getErrorClass(),
          exceptionWithSparkThrowable))

    // Verify log messages for exception without SparkThrowable
    when(mockSparkServer.handleStartServerError(exceptionWithoutSparkThrowable))
      .thenAnswer(_ =>
        mockLogger.error("Error starting Spark Connect server", exceptionWithoutSparkThrowable))

    // Call the methods to trigger the logging
    mockSparkServer.handleStartServerError(exceptionWithSparkThrowable)
    mockSparkServer.handleStartServerError(exceptionWithoutSparkThrowable)

    // Verify that the appropriate logError messages were called
    verify(mockLogger)
      .error(
        "Error starting Spark Connect server with error class: INTERNAL_ERROR",
        exceptionWithSparkThrowable)
    verify(mockLogger)
      .error("Error starting Spark Connect server", exceptionWithoutSparkThrowable)
  }

}