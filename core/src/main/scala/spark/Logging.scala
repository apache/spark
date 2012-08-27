package spark

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Utility trait for classes that want to log data. Creates a SLF4J logger for the class and allows
 * logging messages at different levels using methods that only evaluate parameters lazily if the
 * log level is enabled.
 */
trait Logging {
  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  @transient
  private var log_ : Logger = null

  // Method to get or create the logger for this object
  def log: Logger = {
    if (log_ == null) {
      var className = this.getClass.getName
      // Ignore trailing $'s in the class names for Scala objects
      if (className.endsWith("$")) {
        className = className.substring(0, className.length - 1)
      }
      log_ = LoggerFactory.getLogger(className)
    }
    return log_
  }

  // Log methods that take only a String
  def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }
  
  def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }

  def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  def logWarning(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  def logError(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  // Method for ensuring that logging is initialized, to avoid having multiple
  // threads do it concurrently (as SLF4J initialization is not thread safe).
  def initLogging() { log }
}
