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

package org.apache.spark.repl

import java.util.{Timer, TimerTask}

import org.apache.spark.internal.Logging
import org.apache.spark.repl.Main.{conf, sparkContext}

object InactivityTimeout extends Logging {
  private[repl] var inactivityTimeoutMs = conf.getTimeAsMs("spark.repl.inactivityTimeout", "0s")
  private[repl] var isTest: Boolean = false

  private var inactivityTimer: Option[Timer] = None

  private class InactivityTimerTask extends TimerTask {

    override def run(): Unit = {
      logError(s"Inactivity timeout of $inactivityTimeoutMs ms reached - closing shell")
      if (!isTest) {
        Option(sparkContext).foreach(_.stop)
        System.exit(1)
      }
    }

  }

  def stopInactivityTimer(): Unit = {
    inactivityTimer.foreach(_.cancel())
  }

  def startInactivityTimer(): Unit = {
    if (inactivityTimeoutMs > 0) {
      inactivityTimer.foreach(_.cancel())
      inactivityTimer = Option(new Timer(true))
      inactivityTimer.foreach(_.schedule(new InactivityTimerTask, inactivityTimeoutMs))
    }
  }

}
