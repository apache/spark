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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;

public abstract class SparkLoggerSuiteBase {

  abstract SparkLogger logger();
  abstract String className();
  abstract String logFilePath();

  private File logFile() throws IOException {
    String pwd = new File(".").getCanonicalPath();
    return new File(pwd + File.separator + logFilePath());
  }

  // Return the newly added log contents in the log file after executing the function `f`
  private String captureLogOutput(Runnable func) throws IOException {
    String content = "";
    if (logFile().exists()) {
      content = Files.readString(logFile().toPath());
    }
    func.run();
    String newContent = Files.readString(logFile().toPath());
    return newContent.substring(content.length());
  }

  private String basicMsg() {
    return "This is a log message";
  }

  private final MDC executorIDMDC = MDC.of(LogKeys.EXECUTOR_ID$.MODULE$, "1");
  private final String msgWithMDC = "Lost executor {}.";

  private final MDC[] mdcs = new MDC[] {
      MDC.of(LogKeys.EXECUTOR_ID$.MODULE$, "1"),
      MDC.of(LogKeys.REASON$.MODULE$, "the shuffle data is too large")};
  private final String msgWithMDCs = "Lost executor {}, reason: {}";

  private final MDC executorIDMDCValueIsNull = MDC.of(LogKeys.EXECUTOR_ID$.MODULE$, null);

  private final MDC externalSystemCustomLog =
    MDC.of(CustomLogKeys.CUSTOM_LOG_KEY$.MODULE$, "External system custom log message.");

  // test for basic message (without any mdc)
  abstract String expectedPatternForBasicMsg(Level level);

  // test for basic message and exception
  abstract String expectedPatternForBasicMsgWithException(Level level);

  // test for message (with mdc)
  abstract String expectedPatternForMsgWithMDC(Level level);

  // test for message (with mdcs)
  abstract String expectedPatternForMsgWithMDCs(Level level);

  // test for message (with mdcs and exception)
  abstract String expectedPatternForMsgWithMDCsAndException(Level level);

  // test for message (with mdc - the value is null)
  abstract String expectedPatternForMsgWithMDCValueIsNull(Level level);

  // test for external system custom LogKey
  abstract String expectedPatternForExternalSystemCustomLogKey(Level level);

  @Test
  public void testBasicMsgLogger() {
    Runnable errorFn = () -> logger().error(basicMsg());
    Runnable warnFn = () -> logger().warn(basicMsg());
    Runnable infoFn = () -> logger().info(basicMsg());
    Runnable debugFn = () -> logger().debug(basicMsg());
    Runnable traceFn = () -> logger().trace(basicMsg());
    List.of(
        Pair.of(Level.ERROR, errorFn),
        Pair.of(Level.WARN, warnFn),
        Pair.of(Level.INFO, infoFn),
        Pair.of(Level.DEBUG, debugFn),
        Pair.of(Level.TRACE, traceFn)).forEach(pair -> {
      try {
        assert (captureLogOutput(pair.getRight()).matches(
            expectedPatternForBasicMsg(pair.getLeft())));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void testBasicLoggerWithException() {
    Throwable exception = new RuntimeException("OOM");
    Runnable errorFn = () -> logger().error(basicMsg(), exception);
    Runnable warnFn = () -> logger().warn(basicMsg(), exception);
    Runnable infoFn = () -> logger().info(basicMsg(), exception);
    Runnable debugFn = () -> logger().debug(basicMsg(), exception);
    Runnable traceFn = () -> logger().trace(basicMsg(), exception);
    List.of(
        Pair.of(Level.ERROR, errorFn),
        Pair.of(Level.WARN, warnFn),
        Pair.of(Level.INFO, infoFn),
        Pair.of(Level.DEBUG, debugFn),
        Pair.of(Level.TRACE, traceFn)).forEach(pair -> {
      try {
        assert (captureLogOutput(pair.getRight()).matches(
            expectedPatternForBasicMsgWithException(pair.getLeft())));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void testLoggerWithMDC() {
    Runnable errorFn = () -> logger().error(msgWithMDC, executorIDMDC);
    Runnable warnFn = () -> logger().warn(msgWithMDC, executorIDMDC);
    Runnable infoFn = () -> logger().info(msgWithMDC, executorIDMDC);
    Runnable debugFn = () -> logger().debug(msgWithMDC, executorIDMDC);
    Runnable traceFn = () -> logger().trace(msgWithMDC, executorIDMDC);
    List.of(
        Pair.of(Level.ERROR, errorFn),
        Pair.of(Level.WARN, warnFn),
        Pair.of(Level.INFO, infoFn)).forEach(pair -> {
      try {
        assert (captureLogOutput(pair.getRight()).matches(
            expectedPatternForMsgWithMDC(pair.getLeft())));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void testLoggerWithMDCs() {
    Runnable errorFn = () -> logger().error(msgWithMDCs, mdcs);
    Runnable warnFn = () -> logger().warn(msgWithMDCs, mdcs);
    Runnable infoFn = () -> logger().info(msgWithMDCs, mdcs);
    List.of(
        Pair.of(Level.ERROR, errorFn),
        Pair.of(Level.WARN, warnFn),
        Pair.of(Level.INFO, infoFn)).forEach(pair -> {
      try {
        assert (captureLogOutput(pair.getRight()).matches(
            expectedPatternForMsgWithMDCs(pair.getLeft())));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void testLoggerWithMDCsAndException() {
    Throwable exception = new RuntimeException("OOM");
    Runnable errorFn = () -> logger().error(msgWithMDCs, exception, mdcs);
    Runnable warnFn = () -> logger().warn(msgWithMDCs, exception, mdcs);
    Runnable infoFn = () -> logger().info(msgWithMDCs, exception, mdcs);
    List.of(
        Pair.of(Level.ERROR, errorFn),
        Pair.of(Level.WARN, warnFn),
        Pair.of(Level.INFO, infoFn)).forEach(pair -> {
      try {
        assert (captureLogOutput(pair.getRight()).matches(
            expectedPatternForMsgWithMDCsAndException(pair.getLeft())));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void testLoggerWithMDCValueIsNull() {
    Runnable errorFn = () -> logger().error(msgWithMDC, executorIDMDCValueIsNull);
    Runnable warnFn = () -> logger().warn(msgWithMDC, executorIDMDCValueIsNull);
    Runnable infoFn = () -> logger().info(msgWithMDC, executorIDMDCValueIsNull);
    List.of(
        Pair.of(Level.ERROR, errorFn),
        Pair.of(Level.WARN, warnFn),
        Pair.of(Level.INFO, infoFn)).forEach(pair -> {
      try {
        assert (captureLogOutput(pair.getRight()).matches(
            expectedPatternForMsgWithMDCValueIsNull(pair.getLeft())));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void testLoggerWithExternalSystemCustomLogKey() {
    Runnable errorFn = () -> logger().error("{}", externalSystemCustomLog);
    Runnable warnFn = () -> logger().warn("{}", externalSystemCustomLog);
    Runnable infoFn = () -> logger().info("{}", externalSystemCustomLog);
    List.of(
        Pair.of(Level.ERROR, errorFn),
        Pair.of(Level.WARN, warnFn),
        Pair.of(Level.INFO, infoFn)).forEach(pair -> {
      try {
        assert (captureLogOutput(pair.getRight()).matches(
            expectedPatternForExternalSystemCustomLogKey(pair.getLeft())));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
