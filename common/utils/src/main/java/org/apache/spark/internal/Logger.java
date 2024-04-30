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

public class Logger {

  private final org.slf4j.Logger slf4jLogger;

  Logger(org.slf4j.Logger slf4jLogger) {
    this.slf4jLogger = slf4jLogger;
  }

  public void error(String msg) {
    if (slf4jLogger.isErrorEnabled()) {
      slf4jLogger.error(msg);
    }
  }

  public void error(String msg, Throwable throwable) {
    if (slf4jLogger.isErrorEnabled()) {
      slf4jLogger.error(msg, throwable);
    }
  }

  public void error(String msg, JMDC... mdcs) {
    if (slf4jLogger.isErrorEnabled()) {
      if (mdcs.length == 0) {
        slf4jLogger.error(msg);
      } else {
        withLogContext(msg, mdcs, mwa -> slf4jLogger.error(mwa.message(), mwa.args()));
      }
    }
  }

  public void warn(String msg) {
    if (slf4jLogger.isWarnEnabled()) {
      slf4jLogger.warn(msg);
    }
  }

  public void warn(String msg, Throwable throwable) {
    if (slf4jLogger.isWarnEnabled()) {
      slf4jLogger.warn(msg, throwable);
    }
  }

  public void warn(String msg, JMDC... mdcs) {
    if (slf4jLogger.isWarnEnabled()) {
      if (mdcs.length == 0) {
        slf4jLogger.warn(msg);
      } else {
        withLogContext(msg, mdcs, mwa -> slf4jLogger.warn(mwa.message(), mwa.args()));
      }
    }
  }

  public void info(String msg) {
    if (slf4jLogger.isInfoEnabled()) {
      slf4jLogger.info(msg);
    }
  }

  public void info(String msg, Throwable throwable) {
    if (slf4jLogger.isInfoEnabled()) {
      slf4jLogger.info(msg, throwable);
    }
  }

  public void info(String msg, JMDC... mdcs) {
    if (slf4jLogger.isInfoEnabled()) {
      if (mdcs.length == 0) {
        slf4jLogger.info(msg);
      } else {
        withLogContext(msg, mdcs, mwa -> slf4jLogger.info(mwa.message(), mwa.args()));
      }
    }
  }

  public void debug(String msg) {
    if (slf4jLogger.isDebugEnabled()) {
      slf4jLogger.debug(msg);
    }
  }

  public void debug(String msg, Throwable throwable) {
    if (slf4jLogger.isDebugEnabled()) {
      slf4jLogger.debug(msg, throwable);
    }
  }

  public void debug(String msg, JMDC... mdcs) {
    if (slf4jLogger.isDebugEnabled()) {
      if (mdcs.length == 0) {
        slf4jLogger.debug(msg);
      } else {
        withLogContext(msg, mdcs, mwa -> slf4jLogger.debug(mwa.message(), mwa.args()));
      }
    }
  }

  public void trace(String msg) {
    if (slf4jLogger.isTraceEnabled()) {
      slf4jLogger.trace(msg);
    }
  }

  public void trace(String msg, Throwable throwable) {
    if (slf4jLogger.isTraceEnabled()) {
      slf4jLogger.trace(msg, throwable);
    }
  }

  public void trace(String msg, JMDC... mdcs) {
    if (slf4jLogger.isTraceEnabled()) {
      if (mdcs.length == 0) {
        slf4jLogger.trace(msg);
      } else {
        withLogContext(msg, mdcs, mwa -> slf4jLogger.trace(mwa.message(), mwa.args()));
      }
    }
  }

  protected void withLogContext(
      String message,
      JMDC[] mdcs,
      Consumer<MessageWithArgs> func) {
    Map<String, String> context = new HashMap<>();
    Object[] args = new Object[mdcs.length];
    for (int index = 0; index < mdcs.length; index++) {
      JMDC mdc = mdcs[index];
      args[index] = mdc.value();
      context.put(mdc.key().lowerCaseName(), mdc.value());
    }
    MessageWithArgs messageWithArgs = new MessageWithArgs(message, args);
    try (CloseableThreadContext.Instance ignored = CloseableThreadContext.putAll(context)) {
      func.accept(messageWithArgs);
    }
  }
}
