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

package org.apache.spark.util

import org.apache.commons.lang3.SystemUtils
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
  def register(log: Logger): Unit = synchronized {
    if (SystemUtils.IS_OS_UNIX) {
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
}

private sealed class SignalLoggerHandler(name: String, log: Logger) extends SignalHandler {

  val prevHandler = Signal.handle(new Signal(name), this)

  override def handle(signal: Signal): Unit = {
    log.error("RECEIVED SIGNAL " + signal.getNumber() + ": SIG" + signal.getName())
    prevHandler.handle(signal)
  }
}
