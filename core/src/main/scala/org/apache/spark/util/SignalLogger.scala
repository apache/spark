package org.apache.spark.util

import org.apache.commons.lang.SystemUtils
import org.slf4j.Logger
import sun.misc.{Signal, SignalHandler}

/**
 * Used to log signals received. This can be very useful in debugging crashes or kills.
 *
 * Inspired by Colin Patrick McCabe's similar class from Hadoop.
 */
private[spark] object SignalLogger {

  private var registered = false

  /** Register a signal handler to log signals on UNIX-like systems. */
  def register(log: Logger): Unit = if (SystemUtils.IS_OS_UNIX) {
    require(!registered, "Can't re-install the signal handlers")
    registered = true

    val signals = Seq("TERM", "HUP", "INT")
    for (signal <- signals) {
      try {
        new SignalLoggerHandler(signal, log)
      } catch {
        case e: Exception => log.warn("Failed to register signal handler " + signal, e)
      }
    }
    log.info("Registered signal handlers for [" + signals.mkString(", ") + "]")
  }
}

private sealed class SignalLoggerHandler(name: String, log: Logger) extends SignalHandler {

  val prevHandler = Signal.handle(new Signal(name), this)

  override def handle(signal: Signal): Unit = {
    log.error("RECEIVED SIGNAL " + signal.getNumber() + ": SIG" + signal.getName())
    prevHandler.handle(signal)
  }
}
