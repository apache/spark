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

package org.apache.spark.streaming.scheduler

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext

private[streaming] trait RateLimiter {
  /** To judge whether this RateLimiter is running on the driver side and executor side */
  def isDriver: Boolean

  /** The default controlled ingestion rate set by user */
  def defaultRate: Double

  /**
   * Compute the effective rate according to the processing rate.
   * @param processedRecords number of processed records
   * @param processTimeInMs the total time to process number of processed records.
   */
  def computeEffectiveRate(processedRecords: Long, processTimeInMs: Long): Unit

  /** Get the effective rate */
  def effectiveRate: Double
}

private[streaming]
trait FixedRateLimiter extends RateLimiter {
  def effectiveRate: Double = {
    // If defaultRate is set to 0, choose a large value to make sure processing rate is always
    // faster than receiving rate.
    if (defaultRate == 0.0) Int.MaxValue.toDouble else defaultRate
  }

  def computeEffectiveRate(processedRecords: Long, processTimeInMs: Long): Unit = { }
}

private[streaming]
trait DynamicRateLimiter extends RateLimiter {
  /** A slow start rate set by user to use to control the receiving rate when app is startup */
  def slowStartInitialRate: Double

  protected var dynamicRate = {
    if (defaultRate == 0.0 || slowStartInitialRate > defaultRate)
      slowStartInitialRate
    else
      defaultRate
  }

  def computeEffectiveRate(processedRecords: Long, processTimeInMs: Long): Unit = {
    assert(isDriver)

    val processRate = if (processedRecords == 0L) {
      // If current number of processed records is 0, no processing rate can be got
      dynamicRate
    } else {
      processedRecords.toDouble / processTimeInMs * 1000
    }

    if (defaultRate <= 0) {
      dynamicRate = processRate
    } else {
      dynamicRate = if (processRate > defaultRate) defaultRate else processRate
    }
  }

  def effectiveRate: Double = dynamicRate
}

private[streaming]
object RateLimiter {
  def createReceiverRateLimiter(conf: SparkConf,
      isDriver: Boolean,
      streamId: Int,
      streamingContext: StreamingContext): ReceiverRateLimiter = {
    conf.get("spark.streaming.rateLimiter", "fixed") match {
      case "fixed" =>
        val defaultRate = conf.getInt("spark.streaming.receiver.maxRate", 0).toDouble
        new FixedReceiverRateLimiter(isDriver, streamId, defaultRate, streamingContext)
      case "dynamic" =>
        val defaultRate = conf.getInt("spark.streaming.receiver.maxRate", 0).toDouble
        val initialRate = conf.getInt("spark.streaming.rateLimiter.slowStartRate", 128).toDouble
        new DynamicReceiverRateLimiter(isDriver, streamId, defaultRate, initialRate,
          streamingContext)
      case _ =>
        throw new IllegalArgumentException("Unknown name for RateLimiter")
    }
  }

  def createDirectRateLimiter(conf: SparkConf, streamingContext: StreamingContext)
      : DirectRateLimiter = {
    conf.get("spark.streaming.rateLimiter", "fixed") match {
      case "fixed" =>
        val defaultRate = conf.getInt("spark.streaming.directStream.maxRate", 0).toDouble
        new FixedDirectRateLimiter(defaultRate, streamingContext)
      case "dynamic" =>
        val defaultRate = conf.getInt("spark.streaming.directStream.maxRate", 0).toDouble
        val initialRate = conf.getInt("spark.streaming.rateLimiter.slowStartRate", 128).toDouble
        new DynamicDirectRateLimiter(defaultRate, initialRate, streamingContext)
      case _ =>
        throw new IllegalArgumentException("Unknown name for RateLimiter")
    }
  }
}
