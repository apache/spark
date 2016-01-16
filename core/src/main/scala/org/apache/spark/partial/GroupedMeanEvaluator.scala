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

package org.apache.spark.partial

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.HashMap

import org.apache.spark.util.StatCounter

/**
 * An ApproximateEvaluator for means by key. Returns a map of key to confidence interval.
 */
private[spark] class GroupedMeanEvaluator[T](totalOutputs: Int, confidence: Double)
  extends ApproximateEvaluator[JHashMap[T, StatCounter], Map[T, BoundedDouble]] {

  var outputsMerged = 0
  var sums = new JHashMap[T, StatCounter]   // Sum of counts for each key

  override def merge(outputId: Int, taskResult: JHashMap[T, StatCounter]) {
    outputsMerged += 1
    val iter = taskResult.entrySet.iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val old = sums.get(entry.getKey)
      if (old != null) {
        old.merge(entry.getValue)
      } else {
        sums.put(entry.getKey, entry.getValue)
      }
    }
  }

  override def currentResult(): Map[T, BoundedDouble] = {
    if (outputsMerged == totalOutputs) {
      val result = new JHashMap[T, BoundedDouble](sums.size)
      val iter = sums.entrySet.iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val mean = entry.getValue.mean
        result.put(entry.getKey, new BoundedDouble(mean, 1.0, mean, mean))
      }
      result.asScala
    } else if (outputsMerged == 0) {
      new HashMap[T, BoundedDouble]
    } else {
      val studentTCacher = new StudentTCacher(confidence)
      val result = new JHashMap[T, BoundedDouble](sums.size)
      val iter = sums.entrySet.iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val counter = entry.getValue
        val mean = counter.mean
        val stdev = math.sqrt(counter.sampleVariance / counter.count)
        val confFactor = studentTCacher.get(counter.count)
        val low = mean - confFactor * stdev
        val high = mean + confFactor * stdev
        result.put(entry.getKey, new BoundedDouble(mean, confidence, low, high))
      }
      result.asScala
    }
  }
}
