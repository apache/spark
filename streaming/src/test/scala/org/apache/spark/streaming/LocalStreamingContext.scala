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

import org.scalatest.{BeforeAndAfterEach, Suite}

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

/**
 * Manages a local `ssc` `StreamingContext` variable, correctly stopping it after each test.
 * Note that it also stops active SparkContext if `stopSparkContext` is set to true (default).
 * In most cases you may want to leave it, to isolate environment for SparkContext in each test.
 */
trait LocalStreamingContext extends BeforeAndAfterEach { self: Suite =>

  @transient var ssc: StreamingContext = _
  @transient val stopSparkContext: Boolean = true

  override def afterEach(): Unit = {
    try {
      resetStreamingContext()
    } finally {
      super.afterEach()
    }
  }

  def resetStreamingContext(): Unit = {
    LocalStreamingContext.stop(ssc, stopSparkContext)
    ssc = null
  }
}

object LocalStreamingContext extends Logging {
  def stop(ssc: StreamingContext, stopSparkContext: Boolean): Unit = {
    try {
      if (ssc != null) {
        ssc.stop(stopSparkContext = stopSparkContext)
      }
    } finally {
      if (stopSparkContext) {
        ensureNoActiveSparkContext()
      }
    }
  }

  /**
   * Clean up active SparkContext: try to stop first if there's an active SparkContext.
   * If it fails to stop, log warning message and clear active SparkContext to avoid
   * interfere between tests.
   */
  def ensureNoActiveSparkContext(): Unit = {
    // if SparkContext is still active, try to clean up
    SparkContext.getActive match {
      case Some(sc) =>
        try {
          sc.stop()
        } catch {
          case e: Throwable =>
            logError("Exception trying to stop SparkContext, clear active SparkContext...", e)
            SparkContext.clearActiveContext()
            throw e
        }
      case _ =>
    }
  }

}
