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

package org.apache.spark.mllib.regression

import org.apache.spark.Logging

trait StreamingDecay {
  def getDiscount(numNewDataPoints: Long): Double
}

private[mllib] trait StreamingDecaySetter[T <: StreamingDecaySetter[T]] extends Logging {
  self: T =>
  var decayFactor: Double = 0
  var timeUnit: String = StreamingDecay.BATCHES

  /** Set the decay factor directly (for forgetful algorithms). */
  def setDecayFactor(a: Double): T = {
    this.decayFactor = a
    this
  }

  /** Set the half life and time unit ("batches" or "points") for forgetful algorithms. */
  def setHalfLife(halfLife: Double, timeUnit: String): T = {
    if (timeUnit != StreamingDecay.BATCHES && timeUnit != StreamingDecay.POINTS) {
      throw new IllegalArgumentException("Invalid time unit for decay: " + timeUnit)
    }
    this.decayFactor = math.exp(math.log(0.5) / halfLife)
    logInfo("Setting decay factor to: %g ".format (this.decayFactor))
    this.timeUnit = timeUnit
    this
  }
  
  def getDiscount(numNewDataPoints: Long): Double = timeUnit match {
    case StreamingDecay.BATCHES => decayFactor
    case StreamingDecay.POINTS => math.pow(decayFactor, numNewDataPoints)
  }
}

object StreamingDecay {
  final val BATCHES = "batches"
  final val POINTS = "points"
}