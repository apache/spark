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

import java.util.{Collections, LinkedList}

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

import org.apache.commons.lang3.SystemUtils
import sun.misc.{Signal, SignalHandler}

import org.apache.spark.internal.Logging


/**
 * Contains utilities for working with posix signals.
 */
private[spark] object Signaling extends Logging {

  /**
   * A handler for the given signal that runs a collection of actions.
   */
  private class ActionHandler(signal: Signal) extends SignalHandler {

    private val actions = Collections.synchronizedList(new LinkedList[() => Boolean])

    // original signal handler, before this handler was attached
    private val prevHandler: SignalHandler = Signal.handle(signal, this)

    /**
     * Called when this handler's signal is received. Note that if the same signal is received
     * before this method returns, it is escalated to the previous handler.
     */
    override def handle(sig: Signal): Unit = {
      // register old handler, will receive incoming signals while this handler is running
      Signal.handle(signal, prevHandler)

      val escalate = actions.asScala forall { action =>
        !action()
      }

      if(escalate) {
        prevHandler.handle(sig)
      }

      // re-register this handler
      Signal.handle(signal, this)
    }

    /**
     * Add an action to be run by this handler.
     * @param action An action to be run when a signal is received. Return true if the signal
     * should be stopped with this handler, false if it should be escalated.
     */
    def register(action: => Boolean): Unit = actions.add(() => action)

  }

  // contains association of signals to their respective handlers
  private val handlers = new HashMap[String, ActionHandler]

  /**
   * Adds an action to be run when a given signal is received by this process.
   *
   * Note that signals are only supported on unix-like operating systems and work on a best-effort
   * basis: if a signal is not available or cannot be intercepted, only a warning is emitted.
   *
   * All actions for a given signal are run in a separate thread.
   */
  def register(signal: String)(action: => Boolean): Unit = synchronized {
    if (SystemUtils.IS_OS_UNIX) try {
      val handler = handlers.getOrElseUpdate(signal, {
        val h = new ActionHandler(new Signal(signal))
        logInfo("Registered signal handler for " + signal)
        h
      })
      handler.register(action)
    } catch {
      case ex: Exception => logWarning(s"Failed to register signal handler for " + signal, ex)
    }
  }

}
