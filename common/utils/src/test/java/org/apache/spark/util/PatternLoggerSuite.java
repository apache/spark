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

package org.apache.spark.util;

import org.apache.logging.log4j.Level;

import org.apache.spark.internal.Logger;
import org.apache.spark.internal.LoggerFactory;

public class PatternLoggerSuite extends LoggerSuiteBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(PatternLoggerSuite.class);

  private String toRegexPattern(Level level, String msg) {
    return msg
        .replace("<level>", level.toString())
        .replace("<className>", className());
  }

  @Override
  Logger logger() {
    return LOGGER;
  }

  @Override
  String className() {
    return PatternLoggerSuite.class.getSimpleName();
  }

  @Override
  String logFilePath() {
    return "target/pattern.log";
  }

  @Override
  String expectedPatternForBasicMsg(Level level) {
    return toRegexPattern(level, ".*<level> <className>: This is a log message\n");
  }

  @Override
  String expectedPatternForBasicMsgWithException(Level level) {
    return toRegexPattern(level, """
        .*<level> <className>: This is a log message
        [\\s\\S]*""");
  }

  @Override
  String expectedPatternForMsgWithMDC(Level level) {
    return toRegexPattern(level, ".*<level> <className>: Lost executor 1.\n");
  }

  @Override
  String expectedPatternForMsgWithMDCs(Level level) {
    return toRegexPattern(level,
      ".*<level> <className>: Lost executor 1, reason: the shuffle data is too large\n");
  }

  @Override
  String expectedPatternForMsgWithMDCsAndException(Level level) {
    return toRegexPattern(level,"""
      .*<level> <className>: Lost executor 1, reason: the shuffle data is too large
      [\\s\\S]*""");
  }

  @Override
  String expectedPatternForMsgWithMDCValueIsNull(Level level) {
    return toRegexPattern(level, ".*<level> <className>: Lost executor null.\n");
  }

  @Override
  String expectedPatternForExternalSystemCustomLogKey(Level level) {
    return toRegexPattern(level, ".*<level> <className>: External system custom log message.\n");
  }
}
