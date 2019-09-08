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

package org.apache.spark.sql.hive.thriftserver.cli.operation

import java.io.CharArrayWriter
import java.util.regex.Pattern

import scala.collection.JavaConverters._

import com.google.common.base.Joiner
import org.apache.hadoop.hive.ql.log.PerfLogger
import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.hadoop.hive.ql.session.OperationLog.LoggingLevel
import org.apache.log4j.{PatternLayout, _}
import org.apache.log4j.spi.{Filter, LoggingEvent}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.thriftserver.cli.thrift.CLIServiceUtils

class LogDivertAppender extends WriterAppender with Logging {

  private val LOG = Logger.getLogger(classOf[LogDivertAppender].getName)
  private var operationManager: OperationManager = _
  private var isVerbose: Boolean = _
  private var verboseLayout: Layout = _

  private class NameFilter(var loggingMode: LoggingLevel,
                           var operationManager: OperationManager) extends Filter {

    /* Patterns that are excluded in verbose logging level.
    * Filter out messages coming from log processing classes, or we'll run an infinite loop.
    */
    private val verboseExcludeNamePattern = Pattern.compile(Joiner.on("|").join(Array[AnyRef](LOG.getName, classOf[OperationLog].getName, classOf[OperationManager].getName)))

    /* Patterns that are included in execution logging level.
     * In execution mode, show only select logger messages.
     */
    private val executionIncludeNamePattern = Pattern.compile(Joiner.on("|").join(Array[AnyRef](
      //        "org.apache.hadoop.mapreduce.JobSubmitter",
      //      "org.apache.hadoop.mapreduce.Job", "SessionState", Task.class.getName(),
      //      "org.apache.hadoop.hive.ql.exec.spark.status.SparkJobMonitor"
      //      classOf[SparkContext].getName,
      //      classOf[TaskSetManager].getName,
      //      classOf[DAGScheduler].getName,
      //      classOf[SparkSqlParser].getName,
      //      classOf[CatalystSqlParser].getName,
      "SessionState",
      classOf[SparkExecuteStatementOperation].getName
    )))

    /* Patterns that are included in performance logging level.
     * In performance mode, show execution and performance logger messages.
     */
    private val performanceIncludeNamePattern: Pattern = Pattern.compile(executionIncludeNamePattern.pattern + "|" + classOf[PerfLogger].getName)

    private var namePattern: Pattern = setCurrentNamePattern(loggingMode)

    def setCurrentNamePattern(loggingMode: LoggingLevel): Pattern = {
      if (loggingMode eq OperationLog.LoggingLevel.VERBOSE) {
        verboseExcludeNamePattern
      } else if (loggingMode eq OperationLog.LoggingLevel.EXECUTION) {
        executionIncludeNamePattern
      } else if (loggingMode eq OperationLog.LoggingLevel.PERFORMANCE) {
        performanceIncludeNamePattern
      } else {
        null
      }
    }

    override def decide(ev: LoggingEvent): Int = {
      val log = operationManager.getOperationLogByThread
      val excludeMatches: Boolean = loggingMode eq OperationLog.LoggingLevel.VERBOSE

      if (log == null) {
        return Filter.DENY
      }

      val currentLoggingMode = log.getOpLoggingLevel
      // If logging is disabled, deny everything.
      if (currentLoggingMode == OperationLog.LoggingLevel.NONE) {
        return Filter.DENY
      }

      if (currentLoggingMode ne loggingMode) {
        loggingMode = currentLoggingMode
        this.namePattern = setCurrentNamePattern(loggingMode)
      }

      val isMatch: Boolean = namePattern.matcher(ev.getLoggerName).matches

      if (excludeMatches == isMatch) {
        // Deny if this is black-list filter (excludeMatches = true) and it
        // matched
        // or if this is whitelist filter and it didn't match
        return Filter.DENY
      }

      Filter.NEUTRAL
    }
  }

  /** This is where the log message will go to */
  private val writer: CharArrayWriter = new CharArrayWriter


  private def setLayout(isVerbose: Boolean, lo: Layout): Unit = {
    if (isVerbose)
      if (lo == null) {
        verboseLayout = CLIServiceUtils.verboseLayout
        LOG.info("Cannot find a Layout from a ConsoleAppender. Using default Layout pattern.")
      }
      else {
        //      lo = CLIServiceUtils.nonVerboseLayout;
        //      when not in verbose mode，print log also completely
        verboseLayout = new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n")
      }
    setLayout(verboseLayout)
  }

  private def initLayout(isVerbose: Boolean): Unit = {
    // There should be a ConsoleAppender. Copy its Layout.
    var layout: Layout = null
    Logger.getRootLogger.getAllAppenders.asScala.foreach { ap =>
      if (ap.isInstanceOf[ConsoleAppender]) {
        layout = ap.asInstanceOf[Appender].getLayout
      }
    }
    this.layout =
      Option(layout).getOrElse(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n"))
  }

  def this(operationManager: OperationManager, loggingMode: LoggingLevel) {
    this()
    isVerbose = loggingMode eq OperationLog.LoggingLevel.VERBOSE
    initLayout(isVerbose)
    setWriter(writer)
    setName("KyuubiLogDivertAppender")
    this.operationManager = operationManager
    this.verboseLayout = if (isVerbose) layout else CLIServiceUtils.verboseLayout
    addFilter(new NameFilter(loggingMode, operationManager))
  }


  override def doAppend(event: LoggingEvent): Unit = {
    val log = operationManager.getOperationLogByThread
    // Set current layout depending on the verbose/non-verbose mode.
    if (log != null) {
      val isCurrModeVerbose = log.getOpLoggingLevel eq OperationLog.LoggingLevel.VERBOSE
      // If there is a logging level change from verbose->non-verbose or vice-versa since
      // the last subAppend call, change the layout to preserve consistency.
      if (isCurrModeVerbose != isVerbose) {
        isVerbose = isCurrModeVerbose
        setLayout(isVerbose, verboseLayout)
      }
    }
    super.doAppend(event)
  }

  /**
   * Overrides WriterAppender.subAppend(), which does the real logging. No need
   * to worry about concurrency since log4j calls this synchronously.
   */
  override protected def subAppend(event: LoggingEvent): Unit = {
    super.subAppend(event)
    // That should've gone into our writer. Notify the LogContext.
    val logOutput = writer.toString
    writer.reset()
    val log = operationManager.getOperationLogByThread
    if (log == null) {
      LOG.debug(" ---+++=== Dropped log event from thread " + event.getThreadName)
      return
    }
    log.writeOperationLog(logOutput)
  }
}
