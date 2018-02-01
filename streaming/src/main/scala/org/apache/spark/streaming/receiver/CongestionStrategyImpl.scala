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

import java.util.Random
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf

/**
 * The default congestion strategy.
 * Throttle the block generator to accept a given number of elements per block.
 */
private [streaming] class ThrottleCongestionStrategy(blockGenerator: BlockGenerator)
  extends ThrottlingCongestionStrategy(blockGenerator, blockGenerator.conf) {

  override def onBlockBoundUpdate(bound: Long): Unit = {
    rateLimiter.updateRate(bound)
  }

}

/**
 * A strategy that drops all elements received after the dynamic bound has been reached.
 * @note this strategy it destructive !
 */
private[streaming] class DropCongestionStrategy(conf: SparkConf)
  extends DestructiveCongestionStrategy(conf: SparkConf) {

  private val latestBound = new AtomicLong(-1)
  private def boundInNumberOfElements() = (latestBound.get() * blockIntervalMs / 1000).toInt

  override def onBlockBoundUpdate(bound: Long): Unit = latestBound.set(bound)

  override def restrictCurrentBuffer(currentBuffer: Iterator[Any]): Iterator[Any] = {
    currentBuffer.take(boundInNumberOfElements())
  }

}

/**
 * A strategy that samples a number of elements equal to the dynamic
 * bound per block out of those received at each block interval.
 *
 * @note this strategy it destructive !
 * @note this strategy makes the content f received Spark RDDs non-deterministic !
 */
private[streaming] class SampleCongestionStrategy(conf: SparkConf)
  extends DestructiveCongestionStrategy(conf) {

  private val latestBound = new AtomicLong(-1)
  private def boundInNumberOfElements() = (latestBound.get() * blockIntervalMs / 1000)

  private val rng = new java.util.Random

  override def onBlockBoundUpdate(bound: Long): Unit = latestBound.set(bound)

  override def restrictCurrentBuffer(currentBuffer: Iterator[Any]): Iterator[Any] = {
    import org.apache.spark.util.random.BernoulliSampler
    lazy val samplees = currentBuffer.toList
    def f = boundInNumberOfElements().toDouble / samplees.size

    new BernoulliSampler(f, rng).sample(samplees.to)

  }

}
