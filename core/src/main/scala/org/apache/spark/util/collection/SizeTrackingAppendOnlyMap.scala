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

package org.apache.spark.util.collection

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.util.SizeEstimator
import org.apache.spark.util.collection.SizeTrackingAppendOnlyMap.Sample

/**
 * Append-only map that keeps track of its estimated size in bytes.
 * We sample with a slow exponential back-off using the SizeEstimator to amortize the time,
 * as each call to SizeEstimator can take a sizable amount of time (order of a few milliseconds).
 */
private[spark] class SizeTrackingAppendOnlyMap[K, V] extends AppendOnlyMap[K, V] {

  /**
   * Controls the base of the exponential which governs the rate of sampling.
   * E.g., a value of 2 would mean we sample at 1, 2, 4, 8, ... elements.
   */
  private val SAMPLE_GROWTH_RATE = 1.1

  /** All samples taken since last resetSamples(). Only the last two are used for extrapolation. */
  private val samples = new ArrayBuffer[Sample]()

  /** Total number of insertions and updates into the map since the last resetSamples(). */
  private var numUpdates: Long = _

  /** The value of 'numUpdates' at which we will take our next sample. */
  private var nextSampleNum: Long = _

  /** The average number of bytes per update between our last two samples. */
  private var bytesPerUpdate: Double = _

  resetSamples()

  /** Called after the map grows in size, as this can be a dramatic change for small objects. */
  def resetSamples() {
    numUpdates = 1
    nextSampleNum = 1
    samples.clear()
    takeSample()
  }

  override def update(key: K, value: V): Unit = {
    super.update(key, value)
    numUpdates += 1
    if (nextSampleNum == numUpdates) { takeSample() }
  }

  override def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    val newValue = super.changeValue(key, updateFunc)
    numUpdates += 1
    if (nextSampleNum == numUpdates) { takeSample() }
    newValue
  }

  /** Takes a new sample of the current map's size. */
  def takeSample() {
    samples += Sample(SizeEstimator.estimate(this), numUpdates)
    // Only use the last two samples to extrapolate. If fewer than 2 samples, assume no change.
    bytesPerUpdate = math.max(0, samples.toSeq.reverse match {
      case latest :: previous :: tail =>
        (latest.size - previous.size).toDouble / (latest.numUpdates - previous.numUpdates)
      case _ =>
        0
    })
    nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong
  }

  override protected def growTable() {
    super.growTable()
    resetSamples()
  }

  /** Estimates the current size of the map in bytes. O(1) time. */
  def estimateSize(): Long = {
    assert(samples.nonEmpty)
    val extrapolatedDelta = bytesPerUpdate * (numUpdates - samples.last.numUpdates)
    (samples.last.size + extrapolatedDelta).toLong
  }
}

private object SizeTrackingAppendOnlyMap {
  case class Sample(size: Long, numUpdates: Long)
}
