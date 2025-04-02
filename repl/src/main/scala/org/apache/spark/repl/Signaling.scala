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

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.SignalUtils

private[repl] object Signaling extends Logging {

  /**
   * Register a SIGINT handler, that terminates all active spark jobs or terminates
   * when no jobs are currently running.
   * This makes it possible to interrupt a running shell job by pressing Ctrl+C.
   */
  def cancelOnInterrupt(): Unit = SignalUtils.register("INT") {
    SparkContext.getActive.map { ctx =>
      if (!ctx.statusTracker.getActiveJobIds().isEmpty) {
        logWarning("Cancelling all active jobs, this can take a while. " +
          "Press Ctrl+C again to exit now.")
        ctx.cancelAllJobs()
        true
      } else {
        false
      }
    }.getOrElse(false)
  }

}
