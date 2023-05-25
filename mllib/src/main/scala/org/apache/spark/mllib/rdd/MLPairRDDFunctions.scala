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

package org.apache.spark.mllib.rdd

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.{Aggregator, InterruptibleIterator, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.BoundedPriorityQueue
import org.apache.spark.util.collection.Utils

/**
 * Machine learning specific Pair RDD functions.
 */
class MLPairRDDFunctions[K: ClassTag, V: ClassTag](self: RDD[(K, V)]) extends Serializable {
  /**
   * Returns the top k (largest) elements for each key from this RDD as defined by the specified
   * implicit Ordering[T].
   * If the number of elements for a certain key is less than k, all of them will be returned.
   *
   * @param num k, the number of top elements to return
   * @param ord the implicit ordering for T
   * @return an RDD that contains the top k values for each key
   */
  def topByKey(num: Int)(implicit ord: Ordering[V]): RDD[(K, Array[V])] = {
    val createCombiner = (v: V) => new BoundedPriorityQueue[V](num)(ord) += v
    val mergeValue = (c: BoundedPriorityQueue[V], v: V) => c += v
    val mergeCombiners = (c1: BoundedPriorityQueue[V], c2: BoundedPriorityQueue[V]) => c1 ++= c2

    val aggregator = new Aggregator[K, V, BoundedPriorityQueue[V]](
      self.context.clean(createCombiner),
      self.context.clean(mergeValue),
      self.context.clean(mergeCombiners))

    self.mapPartitions(iter => {
      val context = TaskContext.get()
      new InterruptibleIterator(
        context,
        aggregator
          .combineValuesByKey(iter, context)
          .map { case (k, v) => (k, v.toArray.sorted(ord.reverse)) }
      )
    }, preservesPartitioning = true
    ).reduceByKey { (array1, array2) =>
      val size = math.min(num, array1.length + array2.length)
      val array = Array.ofDim[V](size)
      Utils.mergeOrdered[V](Seq(array1, array2))(ord.reverse).copyToArray(array, 0, size)
      array
    }
  }
}

object MLPairRDDFunctions {
  /** Implicit conversion from a pair RDD to MLPairRDDFunctions. */
  implicit def fromPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): MLPairRDDFunctions[K, V] =
    new MLPairRDDFunctions[K, V](rdd)
}
