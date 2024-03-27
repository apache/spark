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

import scala.collection.JavaConverters._

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{Filter, LifeCycle, LogEvent, Logger => Log4jLogger, LoggerContext}
import org.apache.logging.log4j.core.appender.ConsoleAppender
import org.apache.logging.log4j.core.config.DefaultConfiguration
import org.apache.logging.log4j.core.filter.AbstractFilter

import org.apache.spark.util.SparkClassUtils

object Log4j2Utils {
  @volatile private[internal] var defaultSparkLog4jConfig = false
  @volatile private[internal] var defaultRootLevel: Level = null

  // Return true if the logger has custom configuration. It depends on:
  // 1. If the logger isn't attached with root logger config (i.e., with custom configuration), or
  // 2. the logger level is different to root config level (i.e., it is changed programmatically).
  //
  // Note that if a logger is programmatically changed log level but set to same level
  // as root config level, we cannot tell if it is with custom configuration.
  private def loggerWithCustomConfig(logger: Log4jLogger): Boolean = {
    val rootConfig = LogManager.getRootLogger.asInstanceOf[Log4jLogger].get()
    (logger.get() ne rootConfig) || (logger.getLevel != rootConfig.getLevel())
  }

  private[internal] def initializeLogging(isInterpreter: Boolean,
                                          silent: Boolean,
                                          logName: String): Unit = {
    val rootLogger = LogManager.getRootLogger.asInstanceOf[Log4jLogger]

    // If Log4j 2 is used but is initialized by default configuration,
    // load a default properties file
    // scalastyle:off println
    if (islog4j2DefaultConfigured()) {
      defaultSparkLog4jConfig = true
      val defaultLogProps = "org/apache/spark/log4j2-defaults.properties"
      Option(SparkClassUtils.getSparkClassLoader.getResource(defaultLogProps)) match {
        case Some(url) =>
          val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
          context.setConfigLocation(url.toURI)
          if (!silent) {
            System.err.println(s"Using Spark's default log4j profile: $defaultLogProps")
            Logging.setLogLevelPrinted = true
          }
        case None =>
          System.err.println(s"Spark was unable to load $defaultLogProps")
      }
    }

    if (defaultRootLevel == null) {
      defaultRootLevel = rootLogger.getLevel()
    }

    if (isInterpreter) {
      // Use the repl's main class to define the default log level when running the shell,
      // overriding the root logger's config if they're different.
      val replLogger = LogManager.getLogger(logName).asInstanceOf[Log4jLogger]
      val replLevel = if (loggerWithCustomConfig(replLogger)) {
        replLogger.getLevel()
      } else {
        Level.WARN
      }
      // Update the consoleAppender threshold to replLevel
      if (replLevel != rootLogger.getLevel()) {
        if (!silent) {
          System.err.printf("Setting default log level to \"%s\".\n", replLevel)
          System.err.println("To adjust logging level use sc.setLogLevel(newLevel). " +
            "For SparkR, use setLogLevel(newLevel).")
          Logging.setLogLevelPrinted = true
        }
        Logging.sparkShellThresholdLevel = replLevel
        rootLogger.getAppenders().asScala.foreach {
          case (_, ca: ConsoleAppender) =>
            ca.addFilter(new SparkShellLoggingFilter())
          case _ => // no-op
        }
      }
    }

    // scalastyle:on println
  }

  private[internal] def setLogLevel(l: Level): Unit = {
    val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val config = ctx.getConfiguration()
    val loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME)
    loggerConfig.setLevel(l)
    ctx.updateLoggers()
  }
  private[internal] def uninitialize(): Unit = {
    if (defaultSparkLog4jConfig) {
      defaultSparkLog4jConfig = false
      val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
      context.reconfigure()
    } else {
      val rootLogger = LogManager.getRootLogger().asInstanceOf[Log4jLogger]
      rootLogger.setLevel(defaultRootLevel)
      Logging.sparkShellThresholdLevel = null
    }
  }

  /**
   * Return true if log4j2 is initialized by default configuration which has one
   * appender with error level. See `org.apache.logging.log4j.core.config.DefaultConfiguration`.
   */
  private[spark] def islog4j2DefaultConfigured(): Boolean = {
    val rootLogger = LogManager.getRootLogger.asInstanceOf[Log4jLogger]
    rootLogger.getAppenders.isEmpty ||
      (rootLogger.getAppenders.size() == 1 &&
        rootLogger.getLevel == Level.ERROR &&
        LogManager.getContext.asInstanceOf[LoggerContext]
          .getConfiguration.isInstanceOf[DefaultConfiguration])
  }

  private[spark] class SparkShellLoggingFilter extends AbstractFilter {
    private var status = LifeCycle.State.INITIALIZING

    /**
     * If sparkShellThresholdLevel is not defined, this filter is a no-op.
     * If log level of event is not equal to root level, the event is allowed. Otherwise,
     * the decision is made based on whether the log came from root or some custom configuration
     *
     * @param loggingEvent
     * @return decision for accept/deny log event
     */
    override def filter(logEvent: LogEvent): Filter.Result = {
      if (Logging.sparkShellThresholdLevel == null) {
        Filter.Result.NEUTRAL
      } else if (logEvent.getLevel.isMoreSpecificThan(Logging.sparkShellThresholdLevel)) {
        Filter.Result.NEUTRAL
      } else {
        val logger = LogManager.getLogger(logEvent.getLoggerName).asInstanceOf[Log4jLogger]
        if (loggerWithCustomConfig(logger)) {
          return Filter.Result.NEUTRAL
        }
        Filter.Result.DENY
      }
    }

    override def getState: LifeCycle.State = status

    override def initialize(): Unit = {
      status = LifeCycle.State.INITIALIZED
    }

    override def start(): Unit = {
      status = LifeCycle.State.STARTED
    }

    override def stop(): Unit = {
      status = LifeCycle.State.STOPPED
    }

    override def isStarted: Boolean = status == LifeCycle.State.STARTED

    override def isStopped: Boolean = status == LifeCycle.State.STOPPED
  }
}
