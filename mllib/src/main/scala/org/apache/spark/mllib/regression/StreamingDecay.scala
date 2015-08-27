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
import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Supplies an interface for the discount value in
 * the forgetful update rule in StreamingLinearAlgorithm.
 * Actual implementation is provided in StreamingDecaySetter[T].
 */
@Experimental
trait StreamingDecay {
  /**
   * Derive the discount factor.
   *
   * @param numNewDataPoints number of data points for the RDD arriving at time t.
   * @return Discount factor
   */
  def getDiscount(numNewDataPoints: Long): Double
}

/**
 * :: Experimental ::
 * StreamingDecaySetter provides the concrete implementation
 * of getDiscount in StreamingDecay and setters for decay factor
 * and half-life.
 */
@Experimental
private[mllib] trait StreamingDecaySetter[T] extends Logging {
  self: T =>
  private var decayFactor: Double = 0
  private var timeUnit: String = StreamingDecay.BATCHES

  /**
   * Set the decay factor for the forgetful algorithms.
   * The decay factor should be between 0 and 1, inclusive.
   * decayFactor = 0: only the data from the most recent RDD will be used.
   * decayFactor = 1: all data since the beginning of the DStream will be used.
   * decayFactor is default to zero.
   *
   * @param decayFactor the decay factor
   */
  def setDecayFactor(decayFactor: Double): T = {
    this.decayFactor = decayFactor
    this
  }


  /**
   * Set the half life and time unit ("batches" or "points") for the forgetful algorithm.
   * The half life along with the time unit provides an alternative way to specify decay factor.
   * The decay factor is calculated such that, for data acquired at time t,
   * its contribution by time t + halfLife will have dropped to 0.5.
   * The unit of time can be specified either as batches or points;
   * see StreamingDecay companion object.
   *
   * @param halfLife the half life
   * @param timeUnit the time unit
   */
  def setHalfLife(halfLife: Double, timeUnit: String): T = {
    if (timeUnit != StreamingDecay.BATCHES && timeUnit != StreamingDecay.POINTS) {
      throw new IllegalArgumentException("Invalid time unit for decay: " + timeUnit)
    }
    this.decayFactor = math.exp(math.log(0.5) / halfLife)
    logInfo("Setting decay factor to: %g ".format (this.decayFactor))
    this.timeUnit = timeUnit
    this
  }

  /**
   * Derive the discount factor.
   *
   * @param numNewDataPoints number of data points for the RDD arriving at time t.
   * @return Discount factor
   */
  def getDiscount(numNewDataPoints: Long): Double = timeUnit match {
    case StreamingDecay.BATCHES => decayFactor
    case StreamingDecay.POINTS => math.pow(decayFactor, numNewDataPoints)
  }
}

/**
 * :: Experimental ::
 * Provides the String constants for allowed time unit in the forgetful algorithm.
 */
@Experimental
object StreamingDecay {
  /**
   * Each RDD in the DStream will be treated as 1 time unit.
   *
   */
  final val BATCHES = "batches"
  /**
   * Each data point will be treated as 1 time unit.
   *
   */
  final val POINTS = "points"
}
