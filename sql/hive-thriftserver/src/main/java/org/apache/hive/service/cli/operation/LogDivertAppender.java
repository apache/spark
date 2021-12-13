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

package org.apache.hive.service.cli.operation;
import java.io.CharArrayWriter;
import java.util.Enumeration;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.hadoop.hive.ql.session.OperationLog.LoggingLevel;
import org.apache.hive.service.cli.CLIServiceUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.AbstractWriterAppender;
import org.apache.logging.log4j.core.appender.WriterManager;
import com.google.common.base.Joiner;
import org.apache.logging.log4j.message.Message;

/**
 * An Appender to divert logs from individual threads to the LogObject they belong to.
 */
public class LogDivertAppender extends AbstractWriterAppender<WriterManager> {
  private static final Logger LOG = LogManager.getLogger(LogDivertAppender.class.getName());
  private final OperationManager operationManager;
  private boolean isVerbose;
  private Layout verboseLayout;

  /**
   * A log filter that filters messages coming from the logger with the given names.
   * It be used as a white list filter or a black list filter.
   * We apply black list filter on the Loggers used by the log diversion stuff, so that
   * they don't generate more logs for themselves when they process logs.
   * White list filter is used for less verbose log collection
   */
  private static class NameFilter implements Filter {
    private Pattern namePattern;
    private LoggingLevel loggingMode;
    private OperationManager operationManager;

    /* Patterns that are excluded in verbose logging level.
     * Filter out messages coming from log processing classes, or we'll run an infinite loop.
     */
    private static final Pattern verboseExcludeNamePattern = Pattern.compile(Joiner.on("|")
      .join(new String[] {LOG.getName(), OperationLog.class.getName(),
      OperationManager.class.getName()}));

    /* Patterns that are included in execution logging level.
     * In execution mode, show only select logger messages.
     */
    private static final Pattern executionIncludeNamePattern = Pattern.compile(Joiner.on("|")
      .join(new String[] {"org.apache.hadoop.mapreduce.JobSubmitter",
      "org.apache.hadoop.mapreduce.Job", "SessionState", Task.class.getName(),
      "org.apache.hadoop.hive.ql.exec.spark.status.SparkJobMonitor"}));

    /* Patterns that are included in performance logging level.
     * In performance mode, show execution and performance logger messages.
     */
    private static final Pattern performanceIncludeNamePattern = Pattern.compile(
      executionIncludeNamePattern.pattern() + "|" + PerfLogger.class.getName());

    private void setCurrentNamePattern(OperationLog.LoggingLevel mode) {
      if (mode == OperationLog.LoggingLevel.VERBOSE) {
        this.namePattern = verboseExcludeNamePattern;
      } else if (mode == OperationLog.LoggingLevel.EXECUTION) {
        this.namePattern = executionIncludeNamePattern;
      } else if (mode == OperationLog.LoggingLevel.PERFORMANCE) {
        this.namePattern = performanceIncludeNamePattern;
      }
    }

    NameFilter(
      OperationLog.LoggingLevel loggingMode, OperationManager op) {
      this.operationManager = op;
      this.loggingMode = loggingMode;
      setCurrentNamePattern(loggingMode);
    }

    @Override
    public Result getOnMismatch() {
      return null;
    }

    @Override
    public Result getOnMatch() {
      return null;
    }

    @Override
    public Result filter(org.apache.logging.log4j.core.Logger logger, Level level, Marker marker, String s, Object... objects) {
      return null;
    }

    @Override
    public Result filter(org.apache.logging.log4j.core.Logger logger, Level level, Marker marker, String s, Object o) {
      return null;
    }

    @Override
    public Result filter(org.apache.logging.log4j.core.Logger logger, Level level, Marker marker, String s, Object o, Object o1) {
      return null;
    }

    @Override
    public Result filter(org.apache.logging.log4j.core.Logger logger, Level level, Marker marker, String s, Object o, Object o1, Object o2) {
      return null;
    }

    @Override
    public Result filter(org.apache.logging.log4j.core.Logger logger, Level level, Marker marker, String s, Object o, Object o1, Object o2, Object o3) {
      return null;
    }

    @Override
    public Result filter(org.apache.logging.log4j.core.Logger logger, Level level, Marker marker, String s, Object o, Object o1, Object o2, Object o3, Object o4) {
      return null;
    }

    @Override
    public Result filter(org.apache.logging.log4j.core.Logger logger, Level level, Marker marker, String s, Object o, Object o1, Object o2, Object o3, Object o4, Object o5) {
      return null;
    }

    @Override
    public Result filter(org.apache.logging.log4j.core.Logger logger, Level level, Marker marker, String s, Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6) {
      return null;
    }

    @Override
    public Result filter(org.apache.logging.log4j.core.Logger logger, Level level, Marker marker, String s, Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7) {
      return null;
    }

    @Override
    public Result filter(org.apache.logging.log4j.core.Logger logger, Level level, Marker marker, String s, Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8) {
      return null;
    }

    @Override
    public Result filter(org.apache.logging.log4j.core.Logger logger, Level level, Marker marker, String s, Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8, Object o9) {
      return null;
    }

    @Override
    public Result filter(org.apache.logging.log4j.core.Logger logger, Level level, Marker marker, Object o, Throwable throwable) {
      return null;
    }

    @Override
    public Result filter(org.apache.logging.log4j.core.Logger logger, Level level, Marker marker, Message message, Throwable throwable) {
      return null;
    }

    @Override
    public Result filter(LogEvent logEvent) {
      OperationLog log = operationManager.getOperationLogByThread();
      boolean excludeMatches = (loggingMode == OperationLog.LoggingLevel.VERBOSE);

      if (log == null) {
        return Result.DENY;
      }

      OperationLog.LoggingLevel currentLoggingMode = log.getOpLoggingLevel();
      // If logging is disabled, deny everything.
      if (currentLoggingMode == OperationLog.LoggingLevel.NONE) {
        return Result.DENY;
      }
      // Look at the current session's setting
      // and set the pattern and excludeMatches accordingly.
      if (currentLoggingMode != loggingMode) {
        loggingMode = currentLoggingMode;
        setCurrentNamePattern(loggingMode);
      }

      boolean isMatch = namePattern.matcher(logEvent.getLoggerName()).matches();

      if (excludeMatches == isMatch) {
        // Deny if this is black-list filter (excludeMatches = true) and it
        // matched
        // or if this is whitelist filter and it didn't match
        return Result.DENY;
      }
      return Result.NEUTRAL;
    }

    @Override
    public State getState() {
      return null;
    }

    @Override
    public void initialize() {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public boolean isStarted() {
      return false;
    }

    @Override
    public boolean isStopped() {
      return false;
    }
  }

  /** This is where the log message will go to */
  private final CharArrayWriter writer = new CharArrayWriter();

  private void setLayout(boolean isVerbose, Layout lo) {
    if (isVerbose) {
      if (lo == null) {
        lo = CLIServiceUtils.verboseLayout;
        LOG.info("Cannot find a Layout from a ConsoleAppender. Using default Layout pattern.");
      }
    } else {
      lo = CLIServiceUtils.nonVerboseLayout;
    }
    setLayout(lo);
  }

  private void initLayout(boolean isVerbose) {
    // There should be a ConsoleAppender. Copy its Layout.
    Logger root = Logger.getRootLogger();
    Layout layout = null;

    Enumeration<?> appenders = root.getAllAppenders();
    while (appenders.hasMoreElements()) {
      Appender ap = (Appender) appenders.nextElement();
      if (ap.getClass().equals(ConsoleAppender.class)) {
        layout = ap.getLayout();
        break;
      }
    }
    setLayout(isVerbose, layout);
  }

  public LogDivertAppender(OperationManager operationManager,
    OperationLog.LoggingLevel loggingMode) {
    isVerbose = (loggingMode == OperationLog.LoggingLevel.VERBOSE);
    initLayout(isVerbose);
    setWriter(writer);
    setName("LogDivertAppender");
    this.operationManager = operationManager;
    this.verboseLayout = isVerbose ? layout : CLIServiceUtils.verboseLayout;
    addFilter(new NameFilter(loggingMode, operationManager));
  }

  @Override
  public void doAppend(LoggingEvent event) {
    OperationLog log = operationManager.getOperationLogByThread();

    // Set current layout depending on the verbose/non-verbose mode.
    if (log != null) {
      boolean isCurrModeVerbose = (log.getOpLoggingLevel() == OperationLog.LoggingLevel.VERBOSE);

      // If there is a logging level change from verbose->non-verbose or vice-versa since
      // the last subAppend call, change the layout to preserve consistency.
      if (isCurrModeVerbose != isVerbose) {
        isVerbose = isCurrModeVerbose;
        setLayout(isVerbose, verboseLayout);
      }
    }
    super.doAppend(event);
  }

  /**
   * Overrides WriterAppender.subAppend(), which does the real logging. No need
   * to worry about concurrency since log4j calls this synchronously.
   */
  @Override
  protected void subAppend(LoggingEvent event) {
    super.subAppend(event);
    // That should've gone into our writer. Notify the LogContext.
    String logOutput = writer.toString();
    writer.reset();

    OperationLog log = operationManager.getOperationLogByThread();
    if (log == null) {
      LOG.debug(" ---+++=== Dropped log event from thread " + event.getThreadName());
      return;
    }
    log.writeOperationLog(logOutput);
  }
}
