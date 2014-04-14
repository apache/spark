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

package org.apache.spark.rdd

import scala.language.implicitConversions

import scala.reflect._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Aggregator, InterruptibleIterator, Logging}
import org.apache.spark.util.collection.AppendOnlyMap

/* Implicit conversions */
import org.apache.spark.SparkContext._

/**
 * Extra functions on RDDs that perform only local operations.  These can be used when data has
 * already been partitioned correctly.
 */
private[spark] class PartitionLocalRDDFunctions[K: ClassTag, V: ClassTag](self: RDD[(K, V)])
  extends Logging
  with Serializable {

  /**
   * Cogroup corresponding partitions of `this` and `other`. These two RDDs should have
   * the same number of partitions. Partitions of these two RDDs are cogrouped
   * according to the indexes of partitions. If we have two RDDs and
   * each of them has n partitions, we will cogroup the partition i from `this`
   * with the partition i from `other`.
   * This function will not introduce a shuffling operation.
   */
  def cogroupLocally[W](other: RDD[(K, W)]): RDD[(K, (Seq[V], Seq[W]))] = {
    val cg = self.zipPartitions(other)((iter1:Iterator[(K, V)], iter2:Iterator[(K, W)]) => {
      val map = new AppendOnlyMap[K, Seq[ArrayBuffer[Any]]]

      val update: (Boolean, Seq[ArrayBuffer[Any]]) => Seq[ArrayBuffer[Any]] = (hadVal, oldVal) => {
        if (hadVal) oldVal else Array.fill(2)(new ArrayBuffer[Any])
      }

      val getSeq = (k: K) => {
        map.changeValue(k, update)
      }

      iter1.foreach { kv => getSeq(kv._1)(0) += kv._2 }
      iter2.foreach { kv => getSeq(kv._1)(1) += kv._2 }

      map.iterator
    }).mapValues { case Seq(vs, ws) => (vs.asInstanceOf[Seq[V]], ws.asInstanceOf[Seq[W]])}

    cg
  }

  /**
   * Group the values for each key within a partition of the RDD into a single sequence.
   * This function will not introduce a shuffling operation.
   */
  def groupByKeyLocally(): RDD[(K, Seq[V])] = {
    def createCombiner(v: V) = ArrayBuffer(v)
    def mergeValue(buf: ArrayBuffer[V], v: V) = buf += v
    val aggregator = new Aggregator[K, V, ArrayBuffer[V]](createCombiner, mergeValue, _ ++ _)
    val bufs = self.mapPartitionsWithContext((context, iter) => {
      new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
    }, preservesPartitioning = true)
    bufs.asInstanceOf[RDD[(K, Seq[V])]]
  }

  /**
   * Join corresponding partitions of `this` and `other`.
   * If we have two RDDs and each of them has n partitions,
   * we will join the partition i from `this` with the partition i from `other`.
   * This function will not introduce a shuffling operation.
   */
  def joinLocally[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = {
    cogroupLocally(other).flatMapValues {
      case (vs, ws) => for (v <- vs.iterator; w <- ws.iterator) yield (v, w)
    }
  }
}

private[spark] object PartitionLocalRDDFunctions {
  implicit def rddToPartitionLocalRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) =
    new PartitionLocalRDDFunctions(rdd)
}


