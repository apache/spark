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

import java.util.Collections

import scala.jdk.CollectionConverters._

import org.apache.commons.lang3.SystemUtils
import org.slf4j.Logger
import sun.misc.{Signal, SignalHandler}

import org.apache.spark.internal.{Logging, MDC, MessageWithContext}
import org.apache.spark.internal.LogKeys._

/**
 * Contains utilities for working with posix signals.
 */
private[spark] object SignalUtils extends Logging {

  /** A flag to make sure we only register the logger once. */
  private var loggerRegistered = false

  /** Register a signal handler to log signals on UNIX-like systems. */
  def registerLogger(log: Logger): Unit = synchronized {
    if (!loggerRegistered) {
      Seq("TERM", "HUP", "INT").foreach { sig =>
        SignalUtils.register(sig) {
          log.error("RECEIVED SIGNAL " + sig)
          false
        }
      }
      loggerRegistered = true
    }
  }

  /**
   * Adds an action to be run when a given signal is received by this process.
   *
   * Note that signals are only supported on unix-like operating systems and work on a best-effort
   * basis: if a signal is not available or cannot be intercepted, only a warning is emitted.
   *
   * All actions for a given signal are run in a separate thread.
   */
  def register(signal: String)(action: => Boolean): Unit = {
    if (SystemUtils.IS_OS_UNIX) {
      register(signal, log"Failed to register signal handler for ${MDC(SIGNAL, signal)}",
        logStackTrace = true)(action)
    }
  }

  /**
   * Adds an action to be run when a given signal is received by this process.
   *
   * This method receives failMessage as additional parameter, which would be logged when it fails
   * to register the signal. Here the failures include the cases 1) OS doesn't support signal at
   * all 2) OS doesn't support given signal (Could be possible with non-POSIX signals)
   *
   * All actions for a given signal are run in a separate thread.
   */
  def register(
      signal: String,
      failMessage: MessageWithContext,
      logStackTrace: Boolean = true)(
      action: => Boolean): Unit = synchronized {
    try {
      val handler = handlers.getOrElseUpdate(signal, {
        logInfo(log"Registering signal handler for ${MDC(SIGNAL, signal)}")
        new ActionHandler(new Signal(signal))
      })
      handler.register(action)
    } catch {
      case ex: Exception =>
        if (logStackTrace) {
          logWarning(failMessage, ex)
        } else {
          logWarning(failMessage)
        }
    }
  }

  /**
   * A handler for the given signal that runs a collection of actions.
   */
  private class ActionHandler(signal: Signal) extends SignalHandler {

    /**
     * List of actions upon the signal; the callbacks should return true if the signal is "handled",
     * i.e. should not escalate to the next callback.
     */
    private val actions = Collections.synchronizedList(new java.util.LinkedList[() => Boolean])

    // original signal handler, before this handler was attached
    private val prevHandler: SignalHandler = Signal.handle(signal, this)

    /**
     * Called when this handler's signal is received. Note that if the same signal is received
     * before this method returns, it is escalated to the previous handler.
     */
    override def handle(sig: Signal): Unit = {
      // register old handler, will receive incoming signals while this handler is running
      Signal.handle(signal, prevHandler)

      // Run all actions, escalate to parent handler if no action catches the signal
      // (i.e. all actions return false). Note that calling `map` is to ensure that
      // all actions are run, `forall` is short-circuited and will stop evaluating
      // after reaching a first false predicate.
      val escalate = actions.asScala.map(action => action()).forall(_ == false)
      if (escalate) {
        prevHandler.handle(sig)
      }

      // re-register this handler
      Signal.handle(signal, this)
    }

    /**
     * Adds an action to be run by this handler.
     * @param action An action to be run when a signal is received. Return true if the signal
     *               should be stopped with this handler, false if it should be escalated.
     */
    def register(action: => Boolean): Unit = actions.add(() => action)
  }

  /** Mapping from signal to their respective handlers. */
  private val handlers = new scala.collection.mutable.HashMap[String, ActionHandler]
}
