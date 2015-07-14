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

package org.apache.spark.streaming.scheduler.rate

import org.apache.spark.Logging

/**
 * Implements a proportional-integral-derivative (PID) controller which acts on
 * the speed of ingestion of elements into Spark Streaming.
 *
 * @param batchDurationMillis the batch duration, in milliseconds
 * @param proportional how much the correction should depend on the current
 *        error,
 * @param integral how much the correction should depend on the accumulation
 *        of past errors,
 * @param derivative how much the correction should depend on a prediction
 *        of future errors, based on current rate of change
 */
private[streaming] class PIDRateEstimator(batchIntervalMillis: Long,
    proportional: Double = -1D,
    integral: Double = -.2D,
    derivative: Double = 0D)
  extends RateEstimator with Logging {

  private var init: Boolean = true
  private var latestTime : Long = -1L
  private var latestSpeed : Double = -1D
  private var latestError : Double = -1L

  if (batchIntervalMillis <= 0) logError("Specified batch interval ${batchIntervalMillis} " +
                                        "in PIDRateEstimator is invalid.")

  def compute(time: Long, // in milliseconds
      elements: Long,
      processingDelay: Long, // in milliseconds
      schedulingDelay: Long // in milliseconds
      ): Option[Double] = {

    this.synchronized {
      if (time > latestTime && processingDelay > 0 && batchIntervalMillis > 0) {

        // in seconds, should be close to batchDuration
        val delaySinceUpdate = (time - latestTime).toDouble / 1000

        // in elements/second
        val processingSpeed = elements.toDouble / processingDelay * 1000

        // in elements/second
        val error = latestSpeed - processingSpeed

        // in elements/second
        val sumError = schedulingDelay.toDouble * processingSpeed / batchIntervalMillis

        // in elements/(second ^ 2)
        val dError = (error - latestError) / delaySinceUpdate

        val newSpeed = (latestSpeed + proportional * error +
                                      integral * sumError +
                                      derivative * dError) max 0D
        latestTime = time
        if (init) {
          latestSpeed = processingSpeed
          latestError = 0D
          init = false

          None
        } else {
          latestSpeed = newSpeed
          latestError = error

          Some(newSpeed)
        }
      } else None
    }
  }

}
