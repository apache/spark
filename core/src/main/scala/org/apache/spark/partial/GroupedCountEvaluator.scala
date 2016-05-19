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
import scala.reflect.ClassTag

import org.apache.commons.math3.distribution.NormalDistribution

import org.apache.spark.util.collection.OpenHashMap

/**
 * An ApproximateEvaluator for counts by key. Returns a map of key to confidence interval.
 */
private[spark] class GroupedCountEvaluator[T : ClassTag](totalOutputs: Int, confidence: Double)
  extends ApproximateEvaluator[OpenHashMap[T, Long], Map[T, BoundedDouble]] {

  var outputsMerged = 0
  var sums = new OpenHashMap[T, Long]()   // Sum of counts for each key

  override def merge(outputId: Int, taskResult: OpenHashMap[T, Long]) {
    outputsMerged += 1
    taskResult.foreach { case (key, value) =>
      sums.changeValue(key, value, _ + value)
    }
  }

  override def currentResult(): Map[T, BoundedDouble] = {
    if (outputsMerged == totalOutputs) {
      val result = new JHashMap[T, BoundedDouble](sums.size)
      sums.foreach { case (key, sum) =>
        result.put(key, new BoundedDouble(sum, 1.0, sum, sum))
      }
      result.asScala
    } else if (outputsMerged == 0) {
      new HashMap[T, BoundedDouble]
    } else {
      val p = outputsMerged.toDouble / totalOutputs
      val confFactor = new NormalDistribution().
        inverseCumulativeProbability(1 - (1 - confidence) / 2)
      val result = new JHashMap[T, BoundedDouble](sums.size)
      sums.foreach { case (key, sum) =>
        val mean = (sum + 1 - p) / p
        val variance = (sum + 1) * (1 - p) / (p * p)
        val stdev = math.sqrt(variance)
        val low = mean - confFactor * stdev
        val high = mean + confFactor * stdev
        result.put(key, new BoundedDouble(mean, confidence, low, high))
      }
      result.asScala
    }
  }
}
