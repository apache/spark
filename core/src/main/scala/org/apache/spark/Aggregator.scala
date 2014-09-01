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

package org.apache.spark

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.collection.{AppendOnlyMap, ExternalAppendOnlyMap}

/**
 * :: DeveloperApi ::
 * A set of functions used to aggregate data.
 *
 * @param createCombiner function to create the initial value of the aggregation.
 * @param mergeValue function to merge a new value into the aggregation result.
 * @param mergeCombiners function to merge outputs from multiple mergeValue function.
 */
@DeveloperApi
case class Aggregator[K, V, C] (
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C) {

  private[this] var externalSorting = true
  private[this] var partialAggCheckInterval = 10000
  private[this] var partialAggReduction = 0.5

  private[spark] def withConf(conf: SparkConf): this.type = {
    externalSorting = conf.getBoolean("spark.shuffle.spill", defaultValue = true)
    partialAggCheckInterval = conf.getInt("spark.partialAgg.interval", 10000)
    partialAggReduction = conf.getDouble("spark.partialAgg.reduction", 0.5)
    this
  }

  // Load the configs from SparkEnv if SparkEnv is set (it wouldn't be set in unit tests).
  if (SparkEnv.get != null) {
    withConf(SparkEnv.get.conf)
  }

  @deprecated("use combineValuesByKey with TaskContext argument", "0.9.0")
  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]]): Iterator[(K, C)] =
    combineValuesByKey(iter, null)

  /**
   * Combines values using a (potentially external) hash map and return the combined results, aka
   * partial aggregation.
   *
   * Note that output from this function does not guarantee each key appearing only once. It can
   * choose to not combine values if it doesn't observe any reduction in size with partial
   * aggregation. In the default case, it will go through the first 10000 records and perform
   * partial aggregation. After the first 10000 records, if it doesn't see a reduction factor
   * smaller than 0.5, it will disable partial aggregation and simply output one row per input row
   * for records beyond the first 10000.
   */
  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]],
                         context: TaskContext): Iterator[(K, C)] = {
    if (!externalSorting) {
      val combiners = new AppendOnlyMap[K,C]
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }

      // A flag indicating whether we should do partial aggregation or not.
      var partialAggEnabled = true
      var numRecords = 0
      while (iter.hasNext && partialAggEnabled) {
        kv = iter.next()
        combiners.changeValue(kv._1, update)

        numRecords += 1
        if (numRecords == partialAggCheckInterval) {
          // Disable partial aggregation if we don't see enough reduction.
          val partialAggSize = combiners.size
          if (partialAggSize > numRecords * partialAggReduction) {
            partialAggEnabled = false
          }
        }
      }

      if (!partialAggEnabled && iter.hasNext) {
        // Partial aggregation was turned off because we didn't observe enough reduction.
        combiners.iterator ++ iter.map { kv => (kv._1, createCombiner(kv._2)) }
      } else {
        // We consumed all our records in partial aggregation. Just iterate over the results.
        combiners.iterator
      }
    } else {
      // TODO: disable partial aggregation when reduction factor is not met.
      val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners)
      combiners.insertAll(iter)
      // Update task metrics if context is not null
      // TODO: Make context non optional in a future release
      Option(context).foreach { c =>
        c.taskMetrics.memoryBytesSpilled += combiners.memoryBytesSpilled
        c.taskMetrics.diskBytesSpilled += combiners.diskBytesSpilled
      }
      combiners.iterator
    }
  }

  @deprecated("use combineCombinersByKey with TaskContext argument", "0.9.0")
  def combineCombinersByKey(iter: Iterator[_ <: Product2[K, C]]) : Iterator[(K, C)] =
    combineCombinersByKey(iter, null)

  def combineCombinersByKey(iter: Iterator[_ <: Product2[K, C]], context: TaskContext)
      : Iterator[(K, C)] =
  {
    if (!externalSorting) {
      val combiners = new AppendOnlyMap[K,C]
      var kc: Product2[K, C] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeCombiners(oldValue, kc._2) else kc._2
      }
      while (iter.hasNext) {
        kc = iter.next()
        combiners.changeValue(kc._1, update)
      }
      combiners.iterator
    } else {
      val combiners = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
      while (iter.hasNext) {
        val pair = iter.next()
        combiners.insert(pair._1, pair._2)
      }
      // Update task metrics if context is not null
      // TODO: Make context non-optional in a future release
      Option(context).foreach { c =>
        c.taskMetrics.memoryBytesSpilled += combiners.memoryBytesSpilled
        c.taskMetrics.diskBytesSpilled += combiners.diskBytesSpilled
      }
      combiners.iterator
    }
  }
}
