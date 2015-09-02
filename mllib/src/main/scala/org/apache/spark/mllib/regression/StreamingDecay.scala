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
import org.apache.spark.mllib.regression.StreamingDecay.{TimeUnit, BATCHES, POINTS}

/**
 * :: Experimental ::
 * Supply the discount value for the
 * forgetful update rule in [[StreamingLinearAlgorithm]];
 * The degree of forgetfulness can be specified by the decay factor
 * or the half life.
 *
 */
@Experimental
private[mllib] trait StreamingDecay extends Logging{

  private[this] var decayFactor: Double = 0
  private[this] var timeUnit: TimeUnit = BATCHES

  /**
   * Set the decay factor for the forgetful algorithms.
   * The decay factor should be between 0 and 1, inclusive.
   * decayFactor = 0: only the data from the most recent RDD will be used.
   * decayFactor = 1: all data since the beginning of the DStream will be used.
   * decayFactor is default to zero.
   *
   * @param decayFactor the decay factor
   */
  def setDecayFactor(decayFactor: Double): this.type = {
    this.decayFactor = decayFactor
    this
  }


  /**
   * Set the half life and time unit ("batches" or "points") for the forgetful algorithm.
   * The half life along with the time unit provides an alternative way to specify decay factor.
   * The decay factor is calculated such that, for data acquired at time t,
   * its contribution by time t + halfLife will have dropped by 0.5.
   * The unit of time can be specified either as batches or points.
   *
   * @param halfLife the half life
   * @param timeUnit the time unit
   */
  def setHalfLife(halfLife: Double, timeUnit: TimeUnit): this.type = {
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
object StreamingDecay {
  private[mllib] sealed trait TimeUnit
  /**
   * Each RDD in the DStream will be treated as 1 time unit.
   *
   */
  case object BATCHES extends TimeUnit
  /**
   * Each data point will be treated as 1 time unit.
   *
   */
  case object POINTS extends TimeUnit
}
