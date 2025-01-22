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

package org.apache.spark.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.message.ParameterizedMessageFactory;
// checkstyle.off: RegexpSinglelineJava
import org.slf4j.Logger;
// checkstyle.on: RegexpSinglelineJava

// checkstyle.off: RegexpSinglelineJava
/**
 * Guidelines for the Structured Logging Framework - Java Logging
 * <p>
 *
 * Use the `org.apache.spark.internal.SparkLoggerFactory` to get the logger instance in Java code:
 * Getting Logger Instance:
 *   Instead of using `org.slf4j.LoggerFactory`, use `org.apache.spark.internal.SparkLoggerFactory`
 *   to ensure structured logging.
 * <p>
 *
 * import org.apache.spark.internal.SparkLogger;
 * import org.apache.spark.internal.SparkLoggerFactory;
 * private static final SparkLogger logger = SparkLoggerFactory.getLogger(JavaUtils.class);
 * <p>
 *
 * Logging Messages with Variables:
 *   When logging messages with variables, wrap all the variables with `MDC`s and they will be
 *   automatically added to the Mapped Diagnostic Context (MDC).
 * <p>
 *
 * import org.apache.spark.internal.LogKeys;
 * import org.apache.spark.internal.MDC;
 * logger.error("Unable to delete file for partition {}", MDC.of(LogKeys.PARTITION_ID$.MODULE$, i));
 * <p>
 *
 * Constant String Messages:
 *   For logging constant string messages, use the standard logging methods.
 * <p>
 *
 * logger.error("Failed to abort the writer after failing to write map output.", e);
 * <p>
 *
 * If you want to output logs in `java code` through the structured log framework,
 * you can define `custom LogKey` and use it in `java` code as follows:
 * <p>
 *
 * // To add a `custom LogKey`, implement `LogKey`
 * public static class CUSTOM_LOG_KEY implements LogKey { }
 * import org.apache.spark.internal.MDC;
 * logger.error("Unable to delete key {} for cache", MDC.of(CUSTOM_LOG_KEY, "key"));
 */
// checkstyle.on: RegexpSinglelineJava
public class SparkLogger {

  private static final MessageFactory MESSAGE_FACTORY = ParameterizedMessageFactory.INSTANCE;
  private final Logger slf4jLogger;

  SparkLogger(Logger slf4jLogger) {
    this.slf4jLogger = slf4jLogger;
  }

  public boolean isErrorEnabled() {
    return slf4jLogger.isErrorEnabled();
  }

  public void error(String msg) {
    slf4jLogger.error(msg);
  }

  public void error(String msg, Throwable throwable) {
    slf4jLogger.error(msg, throwable);
  }

  public void error(String msg, MDC... mdcs) {
    if (mdcs == null || mdcs.length == 0) {
      slf4jLogger.error(msg);
    } else if (slf4jLogger.isErrorEnabled()) {
      withLogContext(msg, mdcs, null, mt -> slf4jLogger.error(mt.message));
    }
  }

  public void error(String msg, Throwable throwable, MDC... mdcs) {
    if (mdcs == null || mdcs.length == 0) {
      slf4jLogger.error(msg, throwable);
    } else if (slf4jLogger.isErrorEnabled()) {
      withLogContext(msg, mdcs, throwable, mt -> slf4jLogger.error(mt.message, mt.throwable));
    }
  }

  public boolean isWarnEnabled() {
    return slf4jLogger.isWarnEnabled();
  }

  public void warn(String msg) {
    slf4jLogger.warn(msg);
  }

  public void warn(String msg, Throwable throwable) {
    slf4jLogger.warn(msg, throwable);
  }

  public void warn(String msg, MDC... mdcs) {
    if (mdcs == null || mdcs.length == 0) {
      slf4jLogger.warn(msg);
    } else if (slf4jLogger.isWarnEnabled()) {
      withLogContext(msg, mdcs, null, mt -> slf4jLogger.warn(mt.message));
    }
  }

  public void warn(String msg, Throwable throwable, MDC... mdcs) {
    if (mdcs == null || mdcs.length == 0) {
      slf4jLogger.warn(msg, throwable);
    } else if (slf4jLogger.isWarnEnabled()) {
      withLogContext(msg, mdcs, throwable, mt -> slf4jLogger.warn(mt.message, mt.throwable));
    }
  }

  public boolean isInfoEnabled() {
    return slf4jLogger.isInfoEnabled();
  }

  public void info(String msg) {
    slf4jLogger.info(msg);
  }

  public void info(String msg, Throwable throwable) {
    slf4jLogger.info(msg, throwable);
  }

  public void info(String msg, MDC... mdcs) {
    if (mdcs == null || mdcs.length == 0) {
      slf4jLogger.info(msg);
    } else if (slf4jLogger.isInfoEnabled()) {
      withLogContext(msg, mdcs, null, mt -> slf4jLogger.info(mt.message));
    }
  }

  public void info(String msg, Throwable throwable, MDC... mdcs) {
    if (mdcs == null || mdcs.length == 0) {
      slf4jLogger.info(msg, throwable);
    } else if (slf4jLogger.isInfoEnabled()) {
      withLogContext(msg, mdcs, throwable, mt -> slf4jLogger.info(mt.message, mt.throwable));
    }
  }

  public boolean isDebugEnabled() {
    return slf4jLogger.isDebugEnabled();
  }

  public void debug(String msg) {
    slf4jLogger.debug(msg);
  }

  public void debug(String format, Object arg) {
    slf4jLogger.debug(format, arg);
  }

  public void debug(String format, Object arg1, Object arg2) {
    slf4jLogger.debug(format, arg1, arg2);
  }

  public void debug(String format, Object... arguments) {
    slf4jLogger.debug(format, arguments);
  }

  public void debug(String msg, Throwable throwable) {
    slf4jLogger.debug(msg, throwable);
  }

  public boolean isTraceEnabled() {
    return slf4jLogger.isTraceEnabled();
  }

  public void trace(String msg) {
    slf4jLogger.trace(msg);
  }

  public void trace(String format, Object arg) {
    slf4jLogger.trace(format, arg);
  }

  public void trace(String format, Object arg1, Object arg2) {
    slf4jLogger.trace(format, arg1, arg2);
  }

  public void trace(String format, Object... arguments) {
    slf4jLogger.trace(format, arguments);
  }

  public void trace(String msg, Throwable throwable) {
    slf4jLogger.trace(msg, throwable);
  }

  private void withLogContext(
      String pattern,
      MDC[] mdcs,
      Throwable throwable,
      Consumer<MessageThrowable> func) {
    Map<String, String> context = new HashMap<>();
    Object[] args = new Object[mdcs.length];
    for (int index = 0; index < mdcs.length; index++) {
      MDC mdc = mdcs[index];
      String value = (mdc.value() != null) ? mdc.value().toString() : null;
      if (Logging$.MODULE$.isStructuredLoggingEnabled()) {
        context.put(mdc.key().name(), value);
      }
      args[index] = value;
    }
    MessageThrowable messageThrowable = MessageThrowable.of(
        MESSAGE_FACTORY.newMessage(pattern, args).getFormattedMessage(), throwable);
    try (CloseableThreadContext.Instance ignored = CloseableThreadContext.putAll(context)) {
      func.accept(messageThrowable);
    }
  }

  private record MessageThrowable(String message, Throwable throwable) {
    static MessageThrowable of(String message, Throwable throwable) {
      return new MessageThrowable(message, throwable);
    }
  }

  public Logger getSlf4jLogger() {
    return slf4jLogger;
  }
}
