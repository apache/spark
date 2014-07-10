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

package org.apache.spark.streaming

private[streaming] class ContextWaiter {
  private var error: Throwable = null
  private var stopped: Boolean = false

  def notifyError(e: Throwable) = synchronized {
    error = e
    notifyAll()
  }

  def notifyStop() = synchronized {
    stopped = true
    notifyAll()
  }

  def waitForStopOrError(timeout: Long = -1) = synchronized {
    // If already had error, then throw it
    if (error != null) {
      throw error
    }

    // If not already stopped, then wait
    if (!stopped) {
      if (timeout < 0) wait() else wait(timeout)
      if (error != null) throw error
    }
  }
}
