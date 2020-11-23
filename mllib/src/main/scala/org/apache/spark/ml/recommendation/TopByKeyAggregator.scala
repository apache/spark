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

package org.apache.spark.ml.recommendation

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.util.BoundedPriorityQueue


/**
 * Works on rows of the form (K1, K2, V) where K1 & K2 are IDs and V is the score value. Finds
 * the top `num` K2 items based on the given Ordering.
 */
private[recommendation] class TopByKeyAggregator[K1: TypeTag, K2: TypeTag, V: TypeTag]
  (num: Int, ord: Ordering[(K2, V)])
  extends Aggregator[(K1, K2, V), BoundedPriorityQueue[(K2, V)], Array[(K2, V)]] {

  override def zero: BoundedPriorityQueue[(K2, V)] = new BoundedPriorityQueue[(K2, V)](num)(ord)

  override def reduce(
      q: BoundedPriorityQueue[(K2, V)],
      a: (K1, K2, V)): BoundedPriorityQueue[(K2, V)] = {
    q += {(a._2, a._3)}
  }

  override def merge(
      q1: BoundedPriorityQueue[(K2, V)],
      q2: BoundedPriorityQueue[(K2, V)]): BoundedPriorityQueue[(K2, V)] = {
    q1 ++= q2
  }

  override def finish(r: BoundedPriorityQueue[(K2, V)]): Array[(K2, V)] = {
    r.toArray.sorted(ord.reverse)
  }

  override def bufferEncoder: Encoder[BoundedPriorityQueue[(K2, V)]] = {
    Encoders.kryo[BoundedPriorityQueue[(K2, V)]]
  }

  override def outputEncoder: Encoder[Array[(K2, V)]] = ExpressionEncoder[Array[(K2, V)]]()
}


/**
 * Works on rows of the form (ScrId, DstIds, Scores).
 * Finds the top `num` DstIds and Scores.
 */
private[recommendation] class TopKArrayAggregator(num: Int)
  extends Aggregator[
    (Int, Array[Int], Array[Float]),
    (Array[Int], Array[Float]),
    Array[(Int, Float)]] {

  override def zero: (Array[Int], Array[Float]) = {
    (Array.emptyIntArray, Array.emptyFloatArray)
  }

  override def reduce(
      b: (Array[Int], Array[Float]),
      a: (Int, Array[Int], Array[Float])): (Array[Int], Array[Float]) = {
    merge(b, (a._2, a._3))
  }

  def merge(
      b1: (Array[Int], Array[Float]),
      b2: (Array[Int], Array[Float])): (Array[Int], Array[Float]) = {
    val (ids1, scores1) = b1
    val (ids2, socres2) = b2
    if (ids1.isEmpty) {
      b2
    } else if (ids2.isEmpty) {
      b1
    } else {
      val len1 = ids1.length
      val len2 = ids2.length
      val indices = Array.range(0, len1 + len2)
        .sortBy(i => if (i < len1) -scores1(i) else -socres2(i - len1))
        .take(num)
      (indices.map(i => if (i < len1) ids1(i) else ids2(i - len1)),
        indices.map(i => if (i < len1) scores1(i) else socres2(i - len1)))
    }
  }

  override def finish(reduction: (Array[Int], Array[Float])): Array[(Int, Float)] = {
    reduction._1.zip(reduction._2)
  }

  override def bufferEncoder: Encoder[(Array[Int], Array[Float])] = {
    Encoders.kryo[(Array[Int], Array[Float])]
  }

  override def outputEncoder: Encoder[Array[(Int, Float)]] = {
    ExpressionEncoder[Array[(Int, Float)]]()
  }
}
