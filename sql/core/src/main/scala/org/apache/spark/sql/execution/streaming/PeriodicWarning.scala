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
package org.apache.spark.sql.execution.streaming

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging

/** Trait that adds a method to allow warning to be only printed once in every X seconds */
trait PeriodicWarning { self: Logging =>
  case class CallLocation(className: String, lineNum: Int, threadId: Long)
  private val lastLogTimes = new ConcurrentHashMap[CallLocation, Long]

  /**
   * Log warning only once every `periodSecs`. Use this when you dont want a warning to
   * get printed too often. For example, a warning generated from a fast loop.
   */
  def logPeriodicWarning(periodSecs: Long, msg: => String): Unit = {
    val callLoc = callLocation()
    lastLogTimes.putIfAbsent(callLoc, 0)
    val now = System.currentTimeMillis
    if (now - lastLogTimes.get(callLoc) > periodSecs * 1000) {
      logWarning(msg)
      lastLogTimes.put(callLoc, now)
    }
  }

  private def callLocation(): CallLocation = {
    val th = Thread.currentThread()
    val e = th.getStackTrace.head
    CallLocation(e.getClassName, e.getLineNumber, th.getId)
  }
}
