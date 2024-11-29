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

import scala.jdk.CollectionConverters._

import org.apache.logging.log4j.{CloseableThreadContext, Level, LogManager}
import org.apache.logging.log4j.core.{Filter, LifeCycle, LogEvent, Logger => Log4jLogger, LoggerContext}
import org.apache.logging.log4j.core.appender.ConsoleAppender
import org.apache.logging.log4j.core.config.DefaultConfiguration
import org.apache.logging.log4j.core.filter.AbstractFilter
import org.slf4j.{Logger, LoggerFactory}

import org.apache.spark.internal.Logging.SparkShellLoggingFilter
import org.apache.spark.util.SparkClassUtils

/**
 * Guidelines for the Structured Logging Framework - Scala Logging
 * <p>
 *
 * Use the `org.apache.spark.internal.Logging` trait for logging in Scala code:
 * Logging Messages with Variables:
 *   When logging a message with variables, wrap all the variables with `MDC`s and they will be
 *   automatically added to the Mapped Diagnostic Context (MDC).
 * This allows for structured logging and better log analysis.
 * <p>
 *
 * logInfo(log"Trying to recover app: ${MDC(LogKeys.APP_ID, app.id)}")
 * <p>
 *
 * Constant String Messages:
 *   If you are logging a constant string message, use the log methods that accept a constant
 *   string.
 * <p>
 *
 * logInfo("StateStore stopped")
 * <p>
 *
 * Exceptions:
 *   To ensure logs are compatible with Spark SQL and log analysis tools, avoid
 *   `Exception.printStackTrace()`. Use `logError`, `logWarning`, and `logInfo` methods from
 *   the `Logging` trait to log exceptions, maintaining structured and parsable logs.
 * <p>
 *
 * If you want to output logs in `scala code` through the structured log framework,
 * you can define `custom LogKey` and use it in `scala` code as follows:
 * <p>
 *
 * // To add a `custom LogKey`, implement `LogKey`
 * case object CUSTOM_LOG_KEY extends LogKey
 * import org.apache.spark.internal.MDC;
 * logInfo(log"${MDC(CUSTOM_LOG_KEY, "key")}")
 */

/**
 * Mapped Diagnostic Context (MDC) that will be used in log messages.
 * The values of the MDC will be inline in the log message, while the key-value pairs will be
 * part of the ThreadContext.
 */
case class MDC(key: LogKey, value: Any) {
  require(!value.isInstanceOf[MessageWithContext],
    "the class of value cannot be MessageWithContext")
}

object MDC {
  def of(key: LogKey, value: Any): MDC = MDC(key, value)
}

/**
 * Wrapper class for log messages that include a logging context.
 * This is used as the return type of the string interpolator `LogStringContext`.
 */
case class MessageWithContext(message: String, context: java.util.HashMap[String, String]) {
  def +(mdc: MessageWithContext): MessageWithContext = {
    val resultMap = new java.util.HashMap(context)
    resultMap.putAll(mdc.context)
    MessageWithContext(message + mdc.message, resultMap)
  }

  def stripMargin: MessageWithContext = copy(message = message.stripMargin)
}

/**
 * Companion class for lazy evaluation of the MessageWithContext instance.
 */
class LogEntry(messageWithContext: => MessageWithContext) {
  private lazy val cachedMessageWithContext: MessageWithContext = messageWithContext

  def message: String = cachedMessageWithContext.message

  def context: java.util.HashMap[String, String] = cachedMessageWithContext.context
}

/**
 * Companion object for the wrapper to enable implicit conversions
 */
object LogEntry {
  import scala.language.implicitConversions

  implicit def from(msgWithCtx: => MessageWithContext): LogEntry =
    new LogEntry(msgWithCtx)
}

/**
 * Utility trait for classes that want to log data. Creates a SLF4J logger for the class and allows
 * logging messages at different levels using methods that only evaluate parameters lazily if the
 * log level is enabled.
 */
trait Logging {

  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  @transient private var log_ : Logger = null

  // Method to get the logger name for this object
  protected def logName = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  // Method to get or create the logger for this object
  protected def log: Logger = {
    if (log_ == null) {
      initializeLogIfNecessary(false)
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  implicit class LogStringContext(val sc: StringContext) {
    def log(args: MDC*): MessageWithContext = {
      val processedParts = sc.parts.iterator
      val sb = new StringBuilder(StringContext.processEscapes(processedParts.next()))
      val context = new java.util.HashMap[String, String]()

      args.foreach { mdc =>
        val value = if (mdc.value != null) mdc.value.toString else null
        sb.append(value)
        if (Logging.isStructuredLoggingEnabled) {
          context.put(mdc.key.name, value)
        }

        if (processedParts.hasNext) {
          sb.append(StringContext.processEscapes(processedParts.next()))
        }
      }

      MessageWithContext(sb.toString(), context)
    }
  }

  protected def withLogContext(context: java.util.HashMap[String, String])(body: => Unit): Unit = {
    // put into thread context only when structured logging is enabled
    val closeableThreadContextOpt = if (Logging.isStructuredLoggingEnabled) {
      Some(CloseableThreadContext.putAll(context))
    } else {
      None
    }

    try {
      body
    } finally {
      closeableThreadContextOpt.foreach(_.close())
    }
  }

  // Log methods that take only a String
  protected def logInfo(msg: => String): Unit = {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logInfo(entry: LogEntry): Unit = {
    if (log.isInfoEnabled) {
      withLogContext(entry.context) {
        log.info(entry.message)
      }
    }
  }

  protected def logInfo(entry: LogEntry, throwable: Throwable): Unit = {
    if (log.isInfoEnabled) {
      withLogContext(entry.context) {
        log.info(entry.message, throwable)
      }
    }
  }

  protected def logDebug(msg: => String): Unit = {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logDebug(entry: LogEntry): Unit = {
    if (log.isDebugEnabled) {
      withLogContext(entry.context) {
        log.debug(entry.message)
      }
    }
  }

  protected def logDebug(entry: LogEntry, throwable: Throwable): Unit = {
    if (log.isDebugEnabled) {
      withLogContext(entry.context) {
        log.debug(entry.message, throwable)
      }
    }
  }

  protected def logTrace(msg: => String): Unit = {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logTrace(entry: LogEntry): Unit = {
    if (log.isTraceEnabled) {
      withLogContext(entry.context) {
        log.trace(entry.message)
      }
    }
  }

  protected def logTrace(entry: LogEntry, throwable: Throwable): Unit = {
    if (log.isTraceEnabled) {
      withLogContext(entry.context) {
        log.trace(entry.message, throwable)
      }
    }
  }

  protected def logWarning(msg: => String): Unit = {
    if (log.isWarnEnabled) log.warn(msg)
  }

  protected def logWarning(entry: LogEntry): Unit = {
    if (log.isWarnEnabled) {
      withLogContext(entry.context) {
        log.warn(entry.message)
      }
    }
  }

  protected def logWarning(entry: LogEntry, throwable: Throwable): Unit = {
    if (log.isWarnEnabled) {
      withLogContext(entry.context) {
        log.warn(entry.message, throwable)
      }
    }
  }

  protected def logError(msg: => String): Unit = {
    if (log.isErrorEnabled) log.error(msg)
  }

  protected def logError(entry: LogEntry): Unit = {
    if (log.isErrorEnabled) {
      withLogContext(entry.context) {
        log.error(entry.message)
      }
    }
  }

  protected def logError(entry: LogEntry, throwable: Throwable): Unit = {
    if (log.isErrorEnabled) {
      withLogContext(entry.context) {
        log.error(entry.message, throwable)
      }
    }
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable): Unit = {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable): Unit = {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable): Unit = {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable): Unit = {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable): Unit = {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def isTraceEnabled(): Boolean = {
    log.isTraceEnabled
  }

  protected def initializeLogIfNecessary(isInterpreter: Boolean): Unit = {
    initializeLogIfNecessary(isInterpreter, silent = false)
  }

  protected def initializeLogIfNecessary(
      isInterpreter: Boolean,
      silent: Boolean = false): Boolean = {
    if (!Logging.initialized) {
      Logging.initLock.synchronized {
        if (!Logging.initialized) {
          initializeLogging(isInterpreter, silent)
          return true
        }
      }
    }
    false
  }

  // For testing
  private[spark] def initializeForcefully(isInterpreter: Boolean, silent: Boolean): Unit = {
    initializeLogging(isInterpreter, silent)
  }

  private def initializeLogging(isInterpreter: Boolean, silent: Boolean): Unit = {
    if (Logging.isLog4j2()) {
      val rootLogger = LogManager.getRootLogger.asInstanceOf[Log4jLogger]
      // If Log4j 2 is used but is initialized by default configuration,
      // load a default properties file
      // scalastyle:off println
      if (Logging.defaultSparkLog4jConfig || Logging.islog4j2DefaultConfigured()) {
        Logging.defaultSparkLog4jConfig = true
        val defaultLogProps = if (Logging.isStructuredLoggingEnabled) {
          "org/apache/spark/log4j2-defaults.properties"
        } else {
          "org/apache/spark/log4j2-pattern-layout-defaults.properties"
        }
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

      if (Logging.defaultRootLevel == null) {
        Logging.defaultRootLevel = rootLogger.getLevel()
      }

      if (isInterpreter) {
        // Use the repl's main class to define the default log level when running the shell,
        // overriding the root logger's config if they're different.
        val replLogger = LogManager.getLogger(logName).asInstanceOf[Log4jLogger]
        val replLevel = if (Logging.loggerWithCustomConfig(replLogger)) {
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
    Logging.initialized = true

    // Force a call into slf4j to initialize it. Avoids this happening from multiple threads
    // and triggering this: http://mailman.qos.ch/pipermail/slf4j-dev/2010-April/002956.html
    log
  }
}

private[spark] object Logging {
  @volatile private var initialized = false
  @volatile private var defaultRootLevel: Level = null
  @volatile private var defaultSparkLog4jConfig = false
  @volatile private var structuredLoggingEnabled = true
  @volatile private[spark] var sparkShellThresholdLevel: Level = null
  @volatile private[spark] var setLogLevelPrinted: Boolean = false

  val initLock = new Object()
  try {
    // We use reflection here to handle the case where users remove the
    // jul-to-slf4j bridge order to route their logs to JUL.
    val bridgeClass = SparkClassUtils.classForName("org.slf4j.bridge.SLF4JBridgeHandler")
    bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
    val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
    if (!installed) {
      bridgeClass.getMethod("install").invoke(null)
    }
  } catch {
    case e: ClassNotFoundException => // can't log anything yet so just fail silently
  }

  /**
   * Marks the logging system as not initialized. This does a best effort at resetting the
   * logging system to its initial state so that the next class to use logging triggers
   * initialization again.
   */
  def uninitialize(): Unit = initLock.synchronized {
    if (isLog4j2()) {
      if (defaultSparkLog4jConfig) {
        val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
        context.reconfigure()
      } else {
        val rootLogger = LogManager.getRootLogger().asInstanceOf[Log4jLogger]
        rootLogger.setLevel(defaultRootLevel)
        sparkShellThresholdLevel = null
      }
    }
    this.initialized = false
  }

  private def isLog4j2(): Boolean = {
    // This distinguishes the log4j 1.2 binding, currently
    // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
    // org.apache.logging.slf4j.Log4jLoggerFactory
    "org.apache.logging.slf4j.Log4jLoggerFactory"
      .equals(LoggerFactory.getILoggerFactory.getClass.getName)
  }

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

  /**
   * Enable Structured logging framework.
   */
  private[spark] def enableStructuredLogging(): Unit = {
    structuredLoggingEnabled = true
  }

  /**
   * Disable Structured logging framework.
   */
  private[spark] def disableStructuredLogging(): Unit = {
    structuredLoggingEnabled = false
  }

  /**
   * Return true if Structured logging framework is enabled.
   */
  private[spark] def isStructuredLoggingEnabled: Boolean = {
    structuredLoggingEnabled
  }

  private[spark] class SparkShellLoggingFilter extends AbstractFilter {
    private var status = LifeCycle.State.INITIALIZING

    /**
     * If sparkShellThresholdLevel is not defined, this filter is a no-op.
     * If log level of event is not equal to root level, the event is allowed. Otherwise,
     * the decision is made based on whether the log came from root or some custom configuration
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
