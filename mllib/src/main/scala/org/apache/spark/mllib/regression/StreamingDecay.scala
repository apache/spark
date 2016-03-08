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
import org.apache.spark.annotation.{Since, Experimental}
import org.apache.spark.mllib.regression.StreamingDecay.{BATCHES, POINTS}

/**
 * :: Experimental ::
 * Supply the discount value for the
 * forgetful update rule in [[StreamingLinearAlgorithm]];
 * The degree of forgetfulness can be specified by the decay factor
 * or the half life.
 */
@Experimental
private[mllib] trait StreamingDecay extends Logging{
  private var decayFactor: Double = 0
  private var timeUnit: String = StreamingDecay.BATCHES

  /**
   * Set the decay factor for the forgetful algorithms.
   * The decay factor specifies the decay of
   * the contribution of data from time t-1 to time t.
   * Valid decayFactor ranges from 0 to 1, inclusive.
   * decayFactor = 0: previous data have no contribution to the model at the next time unit.
   * decayFactor = 1: previous data have equal contribution to the model as the data arriving
   * at the next time unit.
   * decayFactor is default to 0.
   * @param decayFactor the decay factor
   */
  @Since("1.6.0")
  def setDecayFactor(decayFactor: Double): this.type = {
    this.decayFactor = decayFactor
    this
  }

  /**
   * Set the half life for the forgetful algorithm.
   * The half life provides an alternative way to specify decay factor.
   * The decay factor is calculated such that, for data acquired at time t,
   * its contribution by time t + halfLife will have dropped by 0.5.
   * Half life > 0 is considered as valid.
   * @param halfLife the half life
   */
  @Since("1.6.0")
  def setHalfLife(halfLife: Double): this.type = {
    this.decayFactor = math.exp(math.log(0.5) / halfLife)
    logInfo("Setting decay factor to: %g ".format (this.decayFactor))
    this
  }

  /**
   * Set the time unit for the forgetful algorithm.
   * BATCHES: Each RDD in the DStream will be treated as 1 time unit.
   * POINTS: Each data point will be treated as 1 time unit.
   * timeUnit is default to BATCHES.
   * @param timeUnit the time unit
   */
  @Since("1.6.0")
  def setTimeUnit(timeUnit: String): this.type = {
    if (timeUnit != StreamingDecay.BATCHES && timeUnit != StreamingDecay.POINTS) {
      throw new IllegalArgumentException("Invalid time unit for decay: " + timeUnit)
    }
    this.timeUnit = timeUnit
    this
  }

  /**
   * Derive the discount factor.
   * @param numNewDataPoints number of data points for the RDD arriving at time t.
   * @return Discount factor
   */
  private[mllib] def getDiscount(numNewDataPoints: Long): Double = timeUnit match {
    case BATCHES => decayFactor
    case POINTS => math.pow(decayFactor, numNewDataPoints)
  }
}

/**
 * :: Experimental ::
 * Provides the String constants for allowed time unit in the forgetful algorithm.
 */
@Experimental
@Since("1.6.0")
object StreamingDecay {
  /**
   * Each RDD in the DStream will be treated as 1 time unit.
   */
  @Since("1.6.0")
  final val BATCHES = "BATCHES"
  /**
   * Each data point will be treated as 1 time unit.
   */
  @Since("1.6.0")
  final val POINTS = "POINTS"
}
