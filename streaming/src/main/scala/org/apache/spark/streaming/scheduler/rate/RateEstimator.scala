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

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingConf._

/**
 * A component that estimates the rate at which an `InputDStream` should ingest
 * records, based on updates at every batch completion.
 *
 * Please see `org.apache.spark.streaming.scheduler.RateController` for more details.
 */
private[streaming] trait RateEstimator extends Serializable {

  /**
   * Computes the number of records the stream attached to this `RateEstimator`
   * should ingest per second, given an update on the size and completion
   * times of the latest batch.
   *
   * @param time The timestamp of the current batch interval that just finished
   * @param elements The number of records that were processed in this batch
   * @param processingDelay The time in ms that took for the job to complete
   * @param schedulingDelay The time in ms that the job spent in the scheduling queue
   */
  def compute(
      time: Long,
      elements: Long,
      processingDelay: Long,
      schedulingDelay: Long): Option[Double]
}

object RateEstimator {

  /**
   * Return a new `RateEstimator` based on the value of
   * `spark.streaming.backpressure.rateEstimator`.
   *
   * The only known and acceptable estimator right now is `pid`.
   *
   * @return An instance of RateEstimator
   * @throws IllegalArgumentException if the configured RateEstimator is not `pid`.
   */
  def create(conf: SparkConf, batchInterval: Duration): RateEstimator =
    conf.get(BACKPRESSURE_RATE_ESTIMATOR) match {
      case "pid" =>
        val proportional = conf.get(BACKPRESSURE_PID_PROPORTIONAL)
        val integral = conf.get(BACKPRESSURE_PID_INTEGRAL)
        val derived = conf.get(BACKPRESSURE_PID_DERIVED)
        val minRate = conf.get(BACKPRESSURE_PID_MIN_RATE)
        new PIDRateEstimator(batchInterval.milliseconds, proportional, integral, derived, minRate)

      case estimator =>
        throw new IllegalArgumentException(s"Unknown rate estimator: $estimator")
    }
}
