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

package org.apache.spark.streaming.receiver

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf

/**
 * These traits provide a strategy to deal w/ a large amount of data seen
 * at a Receiver, possibly ensuing an exhaustion of resources.
 * See SPARK-7398
 * Any long blocking operation in this class will hurt the throughput.
 */
private[streaming] abstract class CongestionStrategy(conf: SparkConf) {

  protected val blockIntervalMs = conf.getTimeAsMs("spark.streaming.blockInterval", "200ms")

  /**
   * Called on every batch interval with the estimated maximum number of
   * elements per second that can been processed based on the processing
   * speed observed over the last batch interval.
   */
  def onBlockBoundUpdate(bound: Long): Unit

}

private [streaming] abstract class DestructiveCongestionStrategy(conf: SparkConf)
  extends CongestionStrategy(conf) {

  /**
   * Given a data buffer intended for a block, return an iterator with an
   * amount appropriate with respect to the back-pressure information
   * provided through `onBlockBoundUpdate`.
   */
  def restrictCurrentBuffer(currentBuffer: Iterator[Any]): Iterator[Any]

}

private[streaming] abstract class ThrottlingCongestionStrategy(
  private[receiver] val rateLimiter: RateLimiter,
  conf: SparkConf)
  extends CongestionStrategy(conf)

object CongestionStrategy {

  /**
   * Return a new CongestionStrategy based on the value of
   * `spark.streaming.backpressure.congestionStrategy`.
   *
   * Intended clients of this factory are receiver-based streams
   *
   * @return An instance of CongestionStrategy
   * @throws IllegalArgumentException if there is a configured CongestionStrategy
   *         that doesn't match any known strategies.
   */
  def create(blockGenerator: BlockGenerator): CongestionStrategy = {
    blockGenerator.conf.get("spark.streaming.backpressure.congestionStrategy", "throttle") match {
      case "drop" => new DropCongestionStrategy(blockGenerator.conf)
      case "sample" => new SampleCongestionStrategy(blockGenerator.conf)
      case "throttle" => new ThrottleCongestionStrategy(blockGenerator)
      case strategy =>
        throw new IllegalArgumentException(s"Unkown congestion strategy: $strategy")
    }
  }

}
