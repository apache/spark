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

import org.apache.log4j.{Level, Logger}
import org.apache.log4j.spi.{Filter, LoggingEvent}

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

class LoggingSuite extends SparkFunSuite {

  test("spark-shell logging filter") {
    val ssf = new SparkShellLoggingFilter()
    val rootLogger = Logger.getRootLogger()
    val originalLevel = rootLogger.getLevel()
    rootLogger.setLevel(Level.INFO)
    val originalThreshold = Logging.sparkShellThresholdLevel
    Logging.sparkShellThresholdLevel = Level.WARN
    try {
      val logger = Logger.getLogger("a.b.c.D")
      val logEvent = new LoggingEvent(logger.getName(), logger, Level.INFO, "Test", null)
      assert(ssf.decide(logEvent) === Filter.DENY)

      // log level is less than threshold level but different from root level
      val logEvent1 = new LoggingEvent(logger.getName(), logger, Level.DEBUG, "Test", null)
      assert(ssf.decide(logEvent1) != Filter.DENY)

      // custom log level configured
      val parentLogger = Logger.getLogger("a.b.c")
      parentLogger.setLevel(Level.INFO)
      assert(ssf.decide(logEvent) != Filter.DENY)

      // log level is greater than or equal to threshold level
      val logger2 = Logger.getLogger("a.b.E")
      val logEvent2 = new LoggingEvent(logger2.getName(), logger2, Level.INFO, "Test", null)
      Utils.setLogLevel(Level.INFO)
      assert(ssf.decide(logEvent2) != Filter.DENY)
    } finally {
      rootLogger.setLevel(originalLevel)
      Logging.sparkShellThresholdLevel = originalThreshold
    }
  }
}
