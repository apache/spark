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

package org.apache.spark.internal

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{Filter, Logger}
import org.apache.logging.log4j.core.impl.Log4jLogEvent.Builder
import org.apache.logging.log4j.message.SimpleMessage

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging.SparkShellLoggingFilter
import org.apache.spark.util.Utils

class LoggingSuite extends SparkFunSuite {

  test("spark-shell logging filter") {
    val ssf = new SparkShellLoggingFilter()
    val rootLogger = LogManager.getRootLogger().asInstanceOf[Logger]
    val originalLevel = rootLogger.getLevel()
    rootLogger.setLevel(Level.INFO)
    val originalThreshold = Logging.sparkShellThresholdLevel
    Logging.sparkShellThresholdLevel = Level.WARN
    try {
      // without custom log level configured
      val logger1 = LogManager.getLogger("a.b.c.D")
        .asInstanceOf[Logger]
      val logEvent1 = new Builder().setLevel(Level.INFO)
        .setLoggerName(logger1.getName()).setMessage(new SimpleMessage("Test")).build()
      assert(ssf.filter(logEvent1) == Filter.Result.DENY)

      // custom log level configured (programmingly)
      val parentLogger = LogManager.getLogger("a.b.c")
        .asInstanceOf[Logger]
      parentLogger.setLevel(Level.DEBUG)
      val logEvent2 = new Builder().setLevel(Level.INFO)
        .setLoggerName(parentLogger.getName()).setMessage(new SimpleMessage("Test")).build()
      assert(ssf.filter(logEvent2) == Filter.Result.NEUTRAL)

      // custom log level configured (by log4j2.properties)
      val jettyLogger = LogManager.getLogger("org.sparkproject.jetty")
        .asInstanceOf[Logger]
      val logEvent3 = new Builder().setLevel(Level.INFO)
        .setLoggerName(jettyLogger.getName()).setMessage(new SimpleMessage("Test")).build()
      assert(ssf.filter(logEvent3) != Filter.Result.DENY)

      // log level is greater than or equal to threshold level
      val logger2 = LogManager.getLogger("a.b.E")
        .asInstanceOf[Logger]
      val logEvent4 = new Builder().setLevel(Level.INFO)
        .setLoggerName(logger2.getName()).setMessage(new SimpleMessage("Test")).build()
      Utils.setLogLevel(Level.INFO)
      assert(ssf.filter(logEvent4) != Filter.Result.DENY)
    } finally {
      rootLogger.setLevel(originalLevel)
      Logging.sparkShellThresholdLevel = originalThreshold
    }
  }
}
