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

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConversions._

/** A set of functions used to aggregate data.
  * 
  * @param createCombiner function to create the initial value of the aggregation.
  * @param mergeValue function to merge a new value into the aggregation result.
  * @param mergeCombiners function to merge outputs from multiple mergeValue function.
  */
case class Aggregator[K, V, C] (
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C) {

  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]]) : Iterator[(K, C)] = {
    val combiners = new JHashMap[K, C]
    for (kv <- iter) {
      val oldC = combiners.get(kv._1)
      if (oldC == null) {
        combiners.put(kv._1, createCombiner(kv._2))
      } else {
        combiners.put(kv._1, mergeValue(oldC, kv._2))
      }
    }
    combiners.iterator
  }

  def combineCombinersByKey(iter: Iterator[(K, C)]) : Iterator[(K, C)] = {
    val combiners = new JHashMap[K, C]
    iter.foreach { case(k, c) =>
      val oldC = combiners.get(k)
      if (oldC == null) {
        combiners.put(k, c)
      } else {
        combiners.put(k, mergeCombiners(oldC, c))
      }
    }
    combiners.iterator
  }
}

